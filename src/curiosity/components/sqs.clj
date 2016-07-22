(ns curiosity.components.sqs
  (:require [schema.core :as s]
            [plumbing.core :refer :all]
            [curiosity.utils :refer [pprint-str select-values]]
            [slingshot.slingshot :refer [try+]]
            [cemerick.bandalore :as sqs]
            [cheshire.core :as json]
            [taoensso.timbre :as log]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async :refer [go thread chan <! <!! >!! go-loop timeout]]
            [clojure.core.async.impl.protocols :refer [ReadPort WritePort Channel]]
            [clj-time.core :as t]
            [curiosity.components.rate-limit :as rl])
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
   fails    :- (s/protocol WritePort)
   clock    :- (s/protocol ReadPort)])

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

(defn closed-chan []
  "Returns a closed channel"
  (let [c (chan)]
    (close! c)
    c))

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
   {disable-reads?  :- s/Bool false}
   {inline-clock-ms :- (s/maybe s/Int) nil}
   {clock-ms        :- (s/maybe s/Int) nil}]

  (let [tuple-chan (chan max-waiting)
        ack-chan   (chan)
        fail-chan  (chan)
        clock-chan (atom (closed-chan))
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
                    (log/error (pprint-str &throw-context)))))
              ;; select over the command channel or a timeout based on sqs queue heuristics
              (<!! (condp = num-messages
                     0 (timeout backoff-time)
                     10 (go :continue)
                     (timeout poll-time)))
              (recur)))))

      ;; when enabled, interleave a clock into the stream
      (when inline-clock-ms
        (go-loop []
          (<! (timeout inline-clock-ms))
          (>!! tuple-chan [::clock (millis)])
          (recur)))

      (when clock-ms
        (swap! clock-chan (chan))
        (go-loop []
          (<! (timeout clock-ms))
          (>!! @clock-chan [::clock (millis)])
          (recur)))

      ;; slurp messages from ack-chan and delete them on sqs
      (go-loop []
        (let [msg (<! ack-chan)]
          (when-not (= msg ::clock)
            (try+
             (sqs/delete sqs-client sqs-queue msg)
             (catch Object e
               (log/error
                (pprint-str (merge &throw-context
                                   {:message (str "Could not ack message " msg)
                                    :queue sqs-queue}))))))
          (recur)))

      ;; slurp messages from fail-chan and mark them visible on sqs
      (go-loop []
        (let [msg (<! fail-chan)]
          (when-not (= msg ::clock)
            (try+
             (sqs/change-message-visibility sqs-client sqs-queue msg (int 0))
             (catch Object e
               (log/error (pprint-str
                           (merge &throw-context
                                  {:message (str "Could not fail message " msg)
                                   :queue sqs-queue}))))))
          (recur))))

    ;; return the channels
    {:messages tuple-chan
     :acks ack-chan
     :fails fail-chan
     :clock @clock-chan}))


(s/defrecord SQSAsyncQueueReader
  [sqs-conn        :- SQSConnPool
   max-waiting     :- s/Int
   wait-time       :- s/Int
   backoff-time    :- s/Int
   poll-time       :- s/Int
   visibility-time :- s/Int
   deserializer    :- s/Any
   clock-ms        :- (s/maybe s/Int)
   inline-clock-ms :- (s/maybe s/Int)
   messages        :- (s/protocol ReadPort)
   acks            :- (s/protocol WritePort)
   fails           :- (s/protocol WritePort)
   clock           :- (s/protocol ReadPort)]

  component/Lifecycle
  (start [this]
    (letk [[messages acks fails clock] (sqs-async-reader this)]
      (assoc this :messages messages
                  :acks acks
                  :fails fails
                  :clock clock)))
  (stop [this]
    (assoc this :messages nil :acks nil :fails nil :clock nil)))

(defnk new-sqs-reader :- SQSAsyncQueueReader
  "Constructs a new SQSAsyncReader component. The system will inject the sqs-conn you need."
  [{sqs-conn        :- SQSConnPool nil}
   {max-waiting     :- s/Int 40}
   {wait-time       :- s/Int 1}
   {backoff-time    :- s/Int 100}
   {poll-time       :- s/Int 100}
   {visibility-time :- s/Int 120}
   {deserializer    :- s/Any json/parse-string}
   {clock-ms        :- s/Int nil}
   {inline-clock-ms :- s/Int nil}
   {disable-reads?  :- s/Bool false}]
  (map->SQSAsyncQueueReader {:max-waiting max-waiting
                             :wait-time wait-time
                             :backoff-time backoff-time
                             :poll-time poll-time
                             :visibility-time visibility-time
                             :deserializer deserializer
                             :disable-reads? disable-reads?
                             :sqs-conn sqs-conn
                             :inline-clock-ms inline-clock-ms
                             :clock-ms clock-ms}))

(defnk new-sqs-acker-failer :- SQSAsyncQueueReader
  "Constructs a neutered SQSAsyncReader component that cannot read messages. The system
   wil inject the sqs-conn you need."
  []
  (map->SQSAsyncQueueReader {:disable-reads? true}))

(defprotocol SQSSend
  (send! [this message]
    "Puts a message onto the SQS queue"))

(defnk sqs-async-sender
  [sqs-conn         :- SQSConnPool
   {serializer      :- s/Any json/generate-string}
   {max-retries     :- s/Int 5}
   {max-buffer-size :- s/Int 10}]
  ;; create a messages channel that also serves as a control channel
  (let [messages (chan max-buffer-size)]
    (thread
      (loop []
        (let [{:keys [type message] :as msg} (<!! messages)
              retries (or (:retries msg) 0)]
          (condp = type
            ;; given send, send a message
            ::send
            (do
              (try
                (sqs/send (:client sqs-conn)
                          (:queue sqs-conn)
                          (serializer message))
                (catch Exception e
                  ;; process retries up to max-retries
                  (if (< retries max-retries)
                    (do
                      (log/info "retrying for the nth time" (inc retries))
                      (>!! messages (update msg :retries (fnil inc 0))))
                    (log/error "failed to send message: " (print-str msg)))))
              (recur))
            ;; given shutdown, exit the loop
            ::shutdown ::shutdown-requested
            (do (log/error "Caught an unknown message to sqs-async-sender: " msg)
                (recur)))))
      ::sqs-async-reader-finished)
    ;; return the channel
    messages))

(defn send!*
  "Send a message via async-sender-chan"
  [async-sender-chan message]
  (>!! async-sender-chan
       {:type ::send
        :message message}))

(s/defn shutdown-sender!
  "Signals to the async sender to shutdown"
  [messages :- (s/protocol WritePort)]
  (>!! messages {::type ::shutdown}))

(s/defrecord SQSAsyncSender
    [sqs-conn   :- SQSConnPool
     messages   :- (s/protocol WritePort)
     serializer :- s/Any]

  component/Lifecycle
  (start [this]
    (if messages
      this
      (assoc this :messages (sqs-async-sender this))))
  (stop [this]
    (shutdown-sender! messages)
    (assoc this messages nil))

  SQSSend
  (send! [this message]
    (send!* messages message)))

(s/defn new-sqs-async-sender :- SQSAsyncSender
  "Create a new sqs sender component.
   At 0 and 1 arities, expects the sqs-conn to be injected."
  ([]
   (new-sqs-async-sender json/generate-string))
  ([serializer :- s/Any]
   (map->SQSAsyncSender {:serializer serializer}))
  ([sqs-conn :- SQSConnPool
    serializer :- s/Any]
   (map->SQSAsyncSender {:serializer serializer
                         :sqs-conn sqs-conn})))


(defmacro do-message!
  {:style/indent 1}
  [reader & body]
  `(let [[~'receipt ~'msg] (<!! (:messages ~reader))]
     (try
       (do ~@body)
       (>!! (:acks ~reader) ~'receipt)
       true
       (catch Throwable t#
         (log/error t#)
         (>!! (:fails ~reader) ~'receipt)
         false))))

(defmacro go-message!
  {:style/indent 1}
  [reader & body]
  `(go
     (let [[~'receipt ~'msg] (<! (:messages ~reader))]
       (try
         (do ~@body)
         (>! (:acks ~reader) ~'receipt)
         true
         (catch Throwable t#
           (log/error t#)
           (>! (:fails ~reader) ~'receipt)
           false)))))

(defnk sqs-worker
  "Creates a worker for a given reader that maps f over batches of messages of up to rate-limit in size that have been rate-limited to rate-limit per period split into up to partitions partitions.
  f is executed in a new thread for each batch."
  [reader     :- SQSAsyncQueueReader
   f          :- (s/pred fn?)
   period     :- s/Int
   rate-limit :- s/Int
   partitions :- s/Int]
  ;; f has the signature [Msgs] -> [Bool]
  (let [wrapper (fn [work]
                  (thread
                    (try
                      (f work)
                      (catch Exception e
                        (log/error "Error executing sqs worker fn: " e)
                        (vec (for [_ work] nil))))))]
    (go
      (let [c (rl/partitioned-rate-limit {:in (:messages reader)
                                          :clock (rl/clock period)
                                          :limit rate-limit
                                          :partitions partitions})]
        (go-loop []
          (let [messages (vec (<!! c))
                results (<! (wrapper (map second messages)))]
            (doall
             (map-indexed (fn [idx x]
                            (if x
                              (>!! (:acks reader) (messages idx))
                              (>!! (:fails reader) (messages idx))))
                          results)))
          (recur)))
      ::started)))

(comment

  (require '[com.stuartsierra.component :as component])

  (def conn (-> (new-sqs-conn-pool
                     {:access-key "AKIAIACJKXPF35KENEJA"
                      :secret-key "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"
                      :max-retries 5
                      :queue-name "dev-following-test"
                      :dead-letter-queue-name "dev-following-test-dlq"})
                component/start))

  (def conn nil)

  (log/handle-uncaught-jvm-exceptions!)

  (def sender (-> (new-sqs-async-sender conn json/generate-string)
                  component/start))

  (def reader (-> (new-sqs-reader {:sqs-conn conn})
                  component/start))

  (<!! (:messages reader))

  (defmacro do-message!
    {:style/indent 1}
    [reader & body]
    `(let [[~'receipt ~'msg] (<!! (:messages ~reader))]
       (try
         (do ~@body)
         (>!! (:acks ~reader) ~'receipt)
         (catch Throwable t#
           (log/error t#)
           (>!! (:fails ~reader) ~'receipt)))))

  (defmacro do-messages!)

  (do-message!! reader
    (prn receipt msg))




  (send! sender {:pika :chu})

  (prn "foo")




  )
