(ns curiosity.components.sqs
  (:require [schema.core :as s]
            [plumbing.core :refer :all]
            [curiosity.utils :refer [pprint-str select-values]]
            [slingshot.slingshot :refer [try+]]
            [cemerick.bandalore :as sqs]
            [taoensso.timbre :as log]
            [cheshire.core :as json]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async :refer [go thread chan <! >! <!! >!! go-loop
                                                  timeout close! alts! poll! alts!!]]
            [clojure.core.async.impl.protocols :refer [ReadPort WritePort Channel]]
            [clj-time.core :as t]
            [curiosity.components.codecs :as codecs]
            [curiosity.components.types :as types]
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

(s/defrecord AsyncQueueReaderUnits
  [messages     :- (s/protocol ReadPort)
   inline-clock :- (s/protocol ReadPort)
   clock        :- (s/protocol ReadPort)
   acks         :- (s/protocol ReadPort)
   fails        :- (s/protocol ReadPort)
   stop         :- (s/protocol ReadPort)])

(s/defrecord AsyncQueueReader
  [messages :- (s/protocol ReadPort)
   acks     :- (s/protocol WritePort)
   fails    :- (s/protocol WritePort)
   clock    :- (s/protocol ReadPort)
   stop     :- (s/protocol WritePort)
   units    :- AsyncQueueReaderUnits])

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
  [{access-key             :- s/Str nil}
   {secret-key             :- s/Str nil}
   {max-retries            :- s/Int nil}
   {queue-name             :- s/Str nil}
   {dead-letter-queue-name :- s/Str nil}]
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

(defnk sqs-async-reader :- AsyncQueueReader
  "Returns a channel that will have messages [msg-id msg] on it, a channel where you can
   ack messages given the id returned from the message channel, a channel where you
   can fail messages given the id returned from the message channel, and a channel that
   when written to, stops all of the threads/goroutines after stop-wait-ms+25 milliseconds.
  The final result of a thread chan be checked via the :units maps of channels."
  [sqs-conn         :- SQSConnPool
   {max-waiting     :- s/Int 40}
   {wait-time       :- s/Int 1}
   {backoff-time    :- s/Int 100}
   {poll-time       :- s/Int 100}
   {visibility-time :- s/Int 120}
   {deserializer    :- types/Fn codecs/loads}
   {disable-reads?  :- s/Bool false}
   {inline-clock-ms :- (s/maybe s/Int) nil}
   {clock-ms        :- (s/maybe s/Int) nil}
   {stop-wait-ms    :- s/Int 5000}]
  (let [tuple-chan (chan max-waiting)
        ack-chan   (chan)
        fail-chan  (chan)
        clock-chan (atom (closed-chan))
        stop-chan (chan 1)
        tuple-stop-chan (chan 1)
        ack-stop-chan (chan 1)
        fail-stop-chan (chan 1)
        inline-clock-stop-chan (chan 1)
        clock-stop-chan (chan 1)
        sqs-client (:client sqs-conn)
        sqs-queue  (:queue sqs-conn)
        stop-channels [tuple-stop-chan
                       ack-stop-chan
                       fail-stop-chan
                       inline-clock-stop-chan
                       clock-stop-chan]
        stop-unit (go
                    ;; get the signal
                    (<! stop-chan)
                    ;; fan out
                    (doseq [ch stop-channels]
                      ;; TODO: what if closed?
                      (>! ch ::stop)
                      (close! ch))
                    ;; wait for completion
                    (<! (timeout (+ stop-wait-ms 25)))
                    ::finished)
        tuple-unit (if disable-reads?
                     ;; disable reads by closing the chan, will provide nil
                     (go
                       (async/close! tuple-chan)
                       ::disabled)
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
                                (log/error "Caught error during deserialization/putting to tuple-chan" &throw-context))))
                           ;; select over the command channel or a timeout based on sqs queue heuristics
                           (if (poll! tuple-stop-chan)
                             (do
                               (close! tuple-chan)
                               ::finished)
                             (do
                               (<!! (condp = num-messages
                                      0 (timeout backoff-time)
                                      10 (go :continue)
                                      (timeout poll-time)))
                               (recur)))))))
        ;; when enabled, interleave a clock into the stream
        inline-clock-unit (if inline-clock-ms
                            (go-loop []
                              (<! (timeout inline-clock-ms))
                              (>! tuple-chan [::clock (millis)])
                              (if (poll! inline-clock-stop-chan)
                                ::finished
                                (recur)))
                            (go ::disabled))
        ;; when enabled, provide a channel with a clock message every clock-ms ms
        clock-unit (if clock-ms
                     (do
                       (swap! clock-chan (chan))
                       (go-loop []
                         (<! (timeout clock-ms))
                         (>! @clock-chan [::clock (millis)])
                         (if (poll! clock-stop-chan)
                           (do
                             (close! @clock-chan)
                             ::finished)
                           (recur))))
                     (go ::disabled))
        ;; slurp messages from ack-chan and delete them on sqs
        ack-unit (go
                   (let [bounded-chan (atom ack-stop-chan)]
                     (loop []
                       (let [[msg ch] (alts! [@bounded-chan ack-chan])]
                         (cond
                           ;; signal to cleanup
                           (and
                            (= ch @bounded-chan)
                            (= ch ack-stop-chan))
                           (do
                             (reset! bounded-chan (timeout stop-wait-ms))
                             (recur))
                           ;; we're done
                           (= ch @bounded-chan)
                           (do
                             (close! ack-chan)
                             ::finished)
                           ;; normal processing
                           :else
                           (do
                             (when-not (= msg ::clock)
                               (try+
                                (sqs/delete sqs-client sqs-queue msg)
                                (catch Object e
                                  (log/error "Failed to ack a message!" (assoc &throw-context :queue sqs-queue)))))
                             (recur)))))))
        ;; slurp messages from fail-chan and mark them visible on sqs
        fail-unit (go
                    (let [bounded-chan (atom fail-stop-chan)]
                      (loop []
                        (let [[msg ch] (alts! [@bounded-chan fail-chan])]
                          (cond
                            ;; signal to cleanup
                            (and
                             (= ch @bounded-chan)
                             (= ch fail-stop-chan))
                            (do
                              (reset! bounded-chan (timeout stop-wait-ms))
                              ;; try to fail excess messages sooner
                              (go-loop [[receipt _] (<! tuple-chan)]
                                (if msg
                                  (do (>! fail-chan receipt)
                                      (recur (<! tuple-chan)))
                                  ::done))
                              (recur))
                            ;; we're done
                            (= ch @bounded-chan)
                            (do
                              (close! fail-chan)
                              ::finished)
                            ;; normal processing
                            :else
                            (do
                              (when-not (= msg ::clock)
                                (try+
                                 (sqs/change-message-visibility sqs-client sqs-queue msg (int 0))
                                 (catch Object e
                                   (log/error "Could not fail message"
                                              (assoc &throw-context
                                                     :queue sqs-queue)))))
                              (recur)))))))]
    ;; return the channels
    {:messages tuple-chan
     :acks ack-chan
     :fails fail-chan
     :clock @clock-chan
     :stop stop-chan
     :units {:messages tuple-unit
             :inline-clock inline-clock-unit
             :clock clock-unit
             :acks ack-unit
             :fails fail-unit
             :stop stop-unit}}))


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
   clock           :- (s/protocol ReadPort)
   stop            :- (s/protocol WritePort)
   units           :- AsyncQueueReaderUnits]

  component/Lifecycle
  (start [this]
    (letk [[messages acks fails clock stop units] (sqs-async-reader this)]
      (assoc this :messages messages
                  :acks acks
                  :fails fails
                  :clock clock
                  :stop stop
                  :units units)))
  (stop [this]
    (>!! stop ::stop)
    (<!! (:stop units))
    (assoc this :messages nil :acks nil :fails nil :clock nil :stop nil :units nil)))

(defnk new-sqs-reader :- SQSAsyncQueueReader
  "Constructs a new SQSAsyncReader component. The system will inject the sqs-conn you need."
  [{sqs-conn        :- SQSConnPool nil}
   {max-waiting     :- s/Int 40}
   {wait-time       :- s/Int 1}
   {backoff-time    :- s/Int 100}
   {poll-time       :- s/Int 100}
   {visibility-time :- s/Int 120}
   {deserializer    :- types/Fn codecs/loads}
   {clock-ms        :- s/Int nil}
   {inline-clock-ms :- s/Int nil}
   {disable-reads?  :- s/Bool false}
   {stop-wait-ms    :- s/Int 5000}]
  (map->SQSAsyncQueueReader {:max-waiting max-waiting
                             :wait-time wait-time
                             :backoff-time backoff-time
                             :poll-time poll-time
                             :visibility-time visibility-time
                             :deserializer deserializer
                             :disable-reads? disable-reads?
                             :sqs-conn sqs-conn
                             :inline-clock-ms inline-clock-ms
                             :clock-ms clock-ms
                             :stop-wait-ms stop-wait-ms}))

(defnk new-sqs-acker-failer :- SQSAsyncQueueReader
  "Constructs a neutered SQSAsyncReader component that cannot read messages. The system
   wil inject the sqs-conn you need."
  []
  (map->SQSAsyncQueueReader {:disable-reads? true}))


(s/defrecord AsyncSender
  [messages :- (s/protocol WritePort)
   stop     :- (s/protocol WritePort)
   unit     :- (s/protocol ReadPort)])

(defnk sqs-async-sender :- AsyncSender
  [sqs-conn         :- SQSConnPool
   {serializer      :- types/Fn codecs/dumps}
   {max-retries     :- s/Int 5}
   {max-buffer-size :- s/Int 100}
   {stop-wait-ms    :- s/Int 5000}
   {label           :- s/Str "async-sender"}]
  ;; create a messages channel that also serves as a control channel
  (let [messages (chan max-buffer-size)
        stop-chan (chan 1)
        bounded-chan (atom stop-chan)
        results
        (thread
          (log/info "Starting sqs-async-sender" {:label label
                                                 :max-retries max-retries
                                                 :max-buffer-size max-buffer-size
                                                 :stop-wait-ms stop-wait-ms})
          (loop [retries 0
                 [message ch] (alts!! [messages @bounded-chan])]
            (cond
              ;; Start shutdown
              (and (= ch @bounded-chan)
                   (= ch stop-chan))
              (do (log/info "Starting shut down of sqs-async-sender!" {:label label
                                                                       :stop-wait-ms stop-wait-ms})
                  (close! messages)
                  (reset! bounded-chan (timeout stop-wait-ms))
                  (recur retries (alts!! [messages @bounded-chan])))
              ;; Finish shutdown
              (= ch @bounded-chan)
              (do (log/info "Completing shut down of sqs-async-sender" {:label label
                                                                        :dropped
                                                                        (<!!
                                                                         (let [i (atom 0)]
                                                                           (go-loop []
                                                                             (if (<! messages)
                                                                               (do (swap! i inc)
                                                                                   (recur))
                                                                               @i))))})
                  ::finished)
              ;; Normal operation
              :else
              (do
                ;; we have to use this retry atom as a flag because you can't recur out of try
                (let [retry (atom false)]
                  (try
                    (sqs/send (:client sqs-conn)
                              (:queue sqs-conn)
                              (serializer message))
                    (reset! retry false)
                    (catch Exception e
                      ;; process retries up to max-retries
                      (if (< retries max-retries)
                        (do
                          (log/debug "sqs-async-sender retrying for the nth time" {:n (inc retries)
                                                                                   :label label
                                                                                   :max-retries max-retries})
                          (reset! retry true))
                        (do
                          (log/error "sqs-async-sender failed to send message: " {:label label
                                                                                  :max-retries max-retries
                                                                                  :message message
                                                                                  :last-error e})
                          (reset! retry false)))))
                  (if @retry
                    (recur (inc retries) [message ch])
                    (recur 0 (alts!! [messages @bounded-chan]))))))))]
    ;; return the channels
    {:messages messages
     :stop     stop-chan
     :unit     results}))


(s/defrecord SQSAsyncSender
    [sqs-conn        :- SQSConnPool
     max-buffer-size :- s/Int
     max-retries     :- s/Int
     messages        :- (s/protocol WritePort)
     stop            :- (s/protocol WritePort)
     unit            :- (s/protocol ReadPort)
     serializer      :- types/Fn]
  component/Lifecycle
  (start [this]
    (if messages
      this
      (let [{:keys [messages stop unit]} (sqs-async-sender this)]
        (assoc this
               :messages messages
               :stop stop
               :unit unit))))
  (stop [this]
    (>!! stop ::stop)
    (<!! unit)
    (assoc this messages nil)))

(defnk new-sqs-sender :- SQSAsyncSender
  [{serializer      :- types/Fn codecs/dumps}
   {sqs-conn        :- SQSConnPool nil}
   {max-buffer-size :- s/Int 100}
   {max-retries     :- s/Int 5}]
  (map->SQSAsyncSender {:serializer serializer
                        :max-buffer-size max-buffer-size
                        :sqs-conn sqs-conn}))

(defmacro stoppable-worker
  {:style/indent 1}
  [type unit take put select label reader & body]
  `(let [stop-chan# (chan)]
     (log/info "Starting sqs worker" {:label ~label
                                      :type ~type})
     (~unit
      (loop []
        (let [[v# ch#] (~select [stop-chan# (:messages ~reader)])]
          (if (= ch# stop-chan#)
            (do (log/info "Stopping sqs worker" {:label ~label
                                                 :type ~type})
                ::finished)
            (let [[~'receipt ~'msg] v#]
              (try
                (do ~@body)
                (~put (:acks ~reader) ~'receipt)
                (catch Throwable t#
                  (log/error "Exception in sqs worker " {:throwable t#
                                                         :worker-type ~type
                                                         :msg ~'msg
                                                         :label ~label})
                  (~put (:fails ~reader) ~'receipt)))
              (recur))))))
     stop-chan#))

(defmacro thread-worker
  [label reader & body]
  `(stoppable-worker :threaded thread <!! >!! alts!! ~label ~reader ~@body))

(defmacro go-worker
  [label reader & body]
  `(stoppable-worker :go-block go <! >! alts! ~label ~reader ~@body))

(defnk sqs-partitioned-worker
  "Creates a worker for a given reader that maps f over batches of messages of up to rate-limit in size that have been rate-limited to rate-limit per period split into up to partitions partitions.
  f is executed in a new thread for each batch."
  [reader     :- SQSAsyncQueueReader
   f          :- (s/pred fn?)
   period     :- s/Int
   rate-limit :- s/Int
   partitions :- s/Int
   stop-chan  :- (s/protocol ReadPort)]
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
          (let [[v ch] (alts! [c stop-chan])]
            (if (= ch stop-chan)
              ::finished
              (let [messages (vec v)
                    results (<! (wrapper (map second messages)))]
                (doall
                 (map-indexed (fn [idx x]
                                (if x
                                  (>! (:acks reader) (messages idx))
                                  (>! (:fails reader) (messages idx))))
                              results))
                (recur))))))
      ::started)))

(defnk auto-failer*
  [reader :- SQSAsyncQueueReader
   stop-chan :- (s/protocol WritePort)]
  (go-loop []
    (let [[msg ch] (alts! [(:messages reader) stop-chan])]
      (if (= ch stop-chan)
        ::finished
        (do
          (let [[receipt _] msg]
            (>! (:fails reader) receipt)
            (recur)))))))


(s/defrecord SQSAutoFailer
    [reader :- SQSAsyncQueueReader
     failer :- (s/protocol ReadPort)
     stop-chan :- (s/protocol WritePort)]

  component/Lifecycle
  (start [this]
    (if failer
      this
      (let [stop-chan (chan 1)]
        (assoc this
               :failer (auto-failer* {:reader reader :stop-chan stop-chan})
               :stop-chan stop-chan))))
  (stop [this]
    (if failer
      (do
        (>!! stop-chan ::stop)
        (assoc this :failer nil :stop-chan nil))
      this)))

(defnk new-auto-failer
  [reader :- SQSAsyncQueueReader]
  (->SQSAutoFailer reader nil nil))


(comment

  (import 'com.amazonaws.auth.profile.ProfileCredentialsProvider)
  (def credentials (-> (ProfileCredentialsProvider.)
                       .getCredentials))
  (def conn (component/start (new-sqs-conn-pool
                              {:access-key (.getAWSAccessKeyId credentials)
                               :secret-key (.getAWSSecretKey credentials)
                               :max-retries 5
                               :queue-name "dev-following-test"
                               :dead-letter-queue-name "dev-following-test-dlq"})))

  (def sender (component/start (new-sqs-sender conn codecs/dumps)))

  (def reader (component/start (new-sqs-reader {:sqs-conn conn :deserializer codecs/loads})))


  (prn (<!! (:messages reader)))


  )
