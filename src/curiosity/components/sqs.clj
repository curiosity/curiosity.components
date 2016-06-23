(ns curiosity.components.sqs
  (:require [schema.core :as s]
            [plumbing.core :refer :all]
            [curiosity.utils :refer [pprint-str select-values]]
            [slingshot.slingshot :refer [try+]]
            [cemerick.bandalore :as sqs]
            [cheshire.core :as json]
            [clojure.tools.logging :refer [error]]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async :refer [go thread chan <! <!! >!! go-loop timeout]]
            [clojure.core.async.impl.protocols :refer [ReadPort WritePort Channel]])
  (:import com.amazonaws.services.sqs.AmazonSQSClient
           com.amazonaws.services.sqs.model.CreateQueueRequest))

(defn create-queue!
  "Get or create the queue and return it"
  [client queue-name]
  (->> (CreateQueueRequest. queue-name)
       (<- (.addAttributesEntry "MessageRetentionPeriod" "1209600"))
       (.createQueue client)
       .getQueueUrl))

(defn get-attributes
  [client queue & attributes]
  (-> (.getQueueAttributes client queue attributes)
      .getAttributes
      (select-values attributes)))

(def queue-arn
  #(-> (get-attributes %1 %2 "QueueArn")
       first))

(defn enable-dead-letter-queue!
  "Assigns dead things from-queue to be moved to to-queue as per params"
  [client from-queue to-queue max-count]
  (.setQueueAttributes client
                       from-queue
                       {"RedrivePolicy"
                        (json/generate-string
                          {"maxReceiveCount" (str max-count)
                           "deadLetterTargetArn" (queue-arn client to-queue)})}))

(s/defrecord AsyncQueueReader
  [messages :- (s/protocol ReadPort)
   acks     :- (s/protocol WritePort)
   fails    :- (s/protocol WritePort) ])

(s/defrecord SQSConnPool
  [access-key             :- s/Str
   secret-key             :- s/Str
   client                 :- AmazonSQSClient
   max-retries            :- s/Int
   queue-name             :- s/Str
   queue                  :- s/Str
   dead-letter-queue-name :- s/Str
   dead-letter-queue      :- s/Str]

  component/Lifecycle
  (start [this]
    (let [client (sqs/create-client access-key secret-key)
          q (create-queue! client queue-name)
          dlq (create-queue! client dead-letter-queue-name)]
      (enable-dead-letter-queue! client q dlq max-retries)
      (assoc this :client client :queue q :dead-letter-queue dlq)))
  (stop [this]
    (assoc this :client nil :queue nil :dead-letter-queue nil)))

(defnk new-sqs-conn-pool :- SQSConnPool
  "Creates a new SQSConnPool component"
  [access-key             :- s/Str
   secret-key             :- s/Str
   max-retries            :- s/Int
   queue-name             :- s/Str
   dead-letter-queue-name :- s/Str]
  (map->SQSConnPool {:access-key access-key
                     :secret-key secret-key
                     :max-retries max-retries
                     :queue-name queue-name
                     :dead-letter-queue-name dead-letter-queue-name}))

;; amazon's java sdk is overly verbose at INFO
;(.setLevel (Logger/getLogger "com.amazonaws") Level/WARNING)

;; This has issues in terms of leaking threads on Clojure 1.5 in the REPL (i.e. when creating
;; threads with channels, then discarding them when the system is re-initialized). This is
;; fixed with better channel usage detection and garbage collection in 1.6. This isn't the world
;; we live in though... In prod, this has no effect, as we don't constantly create/delete systems in
;; the same process.
(defnk sqs-async-reader :- AsyncQueueReader
  "Returns a channel that will have messages [msg-id msg] on it, a channel where you can
   ack messages given the id returned from the message channel, and a channel where you
   can fail messages given the id returned from the message channel."
  [sqs-conn         :- SQSConnPool
   {max-waiting     :- s/Int 40}
   {wait-time       :- s/Int 1}
   {backoff-time    :- s/Int 100}
   {poll-time       :- s/Int 100}
   {visibility-time :- s/Int 120}
   {deserializer    :- s/Any json/parse-string}
   {disable-reads?  :- s/Bool false}]

  (let [tuple-chan (chan max-waiting)
        ack-chan   (chan)
        fail-chan  (chan)
        sqs-client (:client sqs-conn)
        sqs-queue  (:queue sqs-conn)]
    (do
      (if disable-reads?
        ;; disable reads by closing the chan, will provide nil
        (async/close! tuple-chan)
        ;; slurp messages from sqs and put them onto tuple-chan
        (thread
          (loop []
            ;; slurp messages from sqs
            (let [msgs (sqs/receive sqs-client
                                    sqs-queue
                                    :limit (min 10 max-waiting)
                                    :visibility visibility-time
                                    :wait-time-seconds wait-time)
                  num-messages (count msgs)]
              ;; try to deserialize each message and put it on tuple-chan
              (doseq [msg msgs]
                (try+
                  ;; do not attempt string->keyword translation during deserialization
                  ;; there are active attacks on this and it's uacceptable with arbitary
                  ;; input due to implementation deficiencies in clojure's reader
                  (>!! tuple-chan [(:receipt-handle msg) (deserializer (:body msg))])
                  (catch Object e
                    (error (pprint-str &throw-context)))))
              ;; select over the command channel or a timeout based on sqs queue heuristics
              (<!! (condp = num-messages
                     0 (timeout backoff-time)
                     10 (go :continue)
                     (timeout poll-time)))
              (recur)))))

      ;; slurp messages from ack-chan and delete them on sqs
      (go-loop []
        (let [msg (<! ack-chan)]
          (try+
            (sqs/delete sqs-client sqs-queue msg)
            (catch Object e
              (error
                (pprint-str (merge &throw-context
                                   {:message (str "Could not ack message " msg)
                                    :queue sqs-queue})))))
          (recur)))

      ;; slurp messages from fail-chan and mark them visible on sqs
      (go-loop []
        (let [msg (<! fail-chan)]
          (try+
            (sqs/change-message-visibility sqs-client sqs-queue msg (int 0))
            (catch Object e
              (error (pprint-str
                       (merge &throw-context
                              {:message (str "Could not fail message " msg)
                               :queue sqs-queue})))))
          (recur))))

    ;; return the channels
    {:messages tuple-chan
     :acks ack-chan
     :fails fail-chan}))

(s/defrecord SQSAsyncQueueReader
  [sqs-conn        :- SQSConnPool
   max-waiting     :- s/Int
   wait-time       :- s/Int
   backoff-time    :- s/Int
   poll-time       :- s/Int
   visibility-time :- s/Int
   deserializer    :- s/Any
   messages        :- (s/protocol ReadPort)
   acks            :- (s/protocol WritePort)
   fails           :- (s/protocol WritePort)]

  component/Lifecycle
  (start [this]
    (letk [[messages acks fails] (sqs-async-reader this)]
      (assoc this :messages messages
                  :acks acks
                  :fails fails)))
  (stop [this]
    (assoc this :messages nil :acks nil :fails nil)))

(defnk new-sqs-reader :- SQSAsyncQueueReader
  "Constructs a new SQSAsyncReader component. The system will inject the sqs-conn you need."
  [{max-waiting     :- s/Int 40}
   {wait-time       :- s/Int 1}
   {backoff-time    :- s/Int 100}
   {poll-time       :- s/Int 100}
   {visibility-time :- s/Int 120}
   {deserializer    :- s/Any json/parse-string}
   {disable-reads?  :- s/Bool false}]
  (map->SQSAsyncQueueReader {:max-waiting max-waiting
                             :wait-time wait-time
                             :backoff-time backoff-time
                             :poll-time poll-time
                             :visibility-time visibility-time
                             :deserializer deserializer
                             :disable-reads? disable-reads?}))

(defnk new-sqs-acker-failer :- SQSAsyncQueueReader
  "Constructs a neutered SQSAsyncReader component that cannot read messages. The system
   wil inject the sqs-conn you need."
  []
  (map->SQSAsyncQueueReader {:disable-reads? true}))
