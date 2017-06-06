(ns curiosity.components.sns
  (:use plumbing.core)
  (:require [schema.core :as s]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [cheshire.core :as json]
            [clojure.core.async.impl.protocols :refer [WritePort ReadPort]]
            [curiosity.components.types :as types]
            [taoensso.timbre :as log]
            [curiosity.components.codecs :as codecs]
            [curiosity.components.sqs :as sqs]
            [curiosity.utils :refer [forv]]
            [curiosity.components.aws :as aws]
            [cheshire.core :as json]
            [clojure.string :as str])
  (:import com.amazonaws.services.sns.AmazonSNSClient
           com.amazonaws.auth.DefaultAWSCredentialsProviderChain
           com.amazonaws.auth.AWSCredentials
           com.amazonaws.ClientConfiguration
           com.amazonaws.services.sqs.AmazonSQSClient
           curiosity.components.sqs.SQSConnPool
           curiosity.components.aws.AWSClientConfig
           curiosity.components.aws.Credentials))

(s/defrecord SNSClientConfig [max-connections max-error-retry config]
  component/Lifecycle
  (start [this]
    (if config
      this
      (assoc this :config (aws/new-client-config {:max-connections max-connections
                                                  :max-error-retry max-error-retry}))))
  (stop [this]
    (assoc this :config nil)))

(s/defn new-client :- AmazonSNSClient
  ([]
   (new-client (.getCredentials (DefaultAWSCredentialsProviderChain/getInstance))))
  ([creds :- AWSCredentials]
   (new-client creds (aws/new-client-config {})))
  ([creds  :- AWSCredentials
    config :- ClientConfiguration]
   (AmazonSNSClient. creds config)))

(s/defrecord SNSClient
    [creds  :- (s/maybe Credentials)
     config :- (s/maybe SNSClientConfig)
     client :- AmazonSNSClient]
  component/Lifecycle
  (start [this]
    (if client
      this
      (assoc this :client (new-client (:creds creds) (:config config)))))
  (stop [this]
    (when client
      (.shutdown client))
    (assoc this :client nil)))

(def SNSClientT (s/if record? SNSClient AmazonSNSClient))

(s/defn sns-client :- AmazonSNSClient
  "Return an AmazonSNSClient given our component or an actual AmazonSNSClient"
  [obj :- SNSClientT]
  (if (record? obj)
    (:client obj)
    obj))

(s/defn create-topic!* :- (s/maybe s/Str)
  "Idempotently create and return a topic ARN"
  [client :- SNSClientT
   topic  :- s/Str]
  (log/info "Idempotently creating SNS topic" {:topic topic})
  (if (aws/arn? topic)
    topic
    (some-> (.createTopic (sns-client client) topic)
            .getTopicArn)))

(def create-topic! (memoize create-topic!*))

(defnk sns-publish! :- (s/maybe s/Str)
  "Synchronously publish a message to AWS SNS; supports retries"
  [client       :- SNSClientT
   topic        :- s/Str
   message      :- s/Any
   {serializer  :- types/Fn codecs/b64-json-dumps}
   {label       :- s/Str "anon-sync"}]
  (when-let [r (->> (serializer message)
                    (.publish (sns-client client) (create-topic! client topic)))]
    (if (log/may-log? :trace)
      (log/trace "Published a message: " {:msg message, :result r, :label label})
      (log/debug "Published a message: " {:msg-id (.getMessageId r), :label label}))
    (.getMessageId r)))

(defnk sns-publisher :- {:msgs (s/protocol WritePort)
                         :stop (s/protocol WritePort)
                         :unit (s/protocol ReadPort)}
  [client           :- SNSClientT
   topic            :- s/Str
   {max-buffer-size :- s/Int 10}
   {stop-wait-ms    :- s/Int 5000}
   {serializer      :- types/Fn codecs/b64-json-dumps}
   {label           :- s/Str "async-sender"}]
  (let [messages     (async/chan max-buffer-size)
        stop-chan    (async/chan 1)
        bounded-chan (atom stop-chan)
        cli          (sns-client client)
        results
        (async/thread
          (loop []
            (let [[message ch] (async/alts!! [messages @stop-chan])]
              (cond
                ;; Start shutdown
                (and (= ch @bounded-chan)
                     (= ch stop-chan))
                (do (log/info "Starting shut down of sns-async-sender!" {:label label
                                                                         :stop-wait-ms stop-wait-ms})
                    (async/close! messages)
                    (reset! bounded-chan (async/timeout stop-wait-ms))
                    (recur))
                ;; Finish Shutdown
                (= ch @bounded-chan)
                (do (log/info "Completing shut down of sns-async-sender" {:label label
                                                                          :dropped
                                                                          (async/<!!
                                                                           (async/go-loop [i 0]
                                                                             (if (async/<! messages)
                                                                               (recur (inc i))
                                                                               i)))})
                    ::finished)
                ;; Normal operation
                :else
                (do
                  (sns-publish! {:client      cli
                                 :topic       topic
                                 :message     message
                                 :serializer  serializer
                                 :label       label})
                  (recur))))))]
    {:msgs messages
     :stop stop-chan
     :unit results}))

(defnk subscribe-to-topic!
  [client    :- SNSClientT
   topic     :- s/Str
   protocol  :- (s/enum :http :https :email :email-json :sms :sqs :application :lambda)
   thing     :- s/Str]
  (log/info "Subcribing to SNS topic" {:topic topic, :protocol protocol, :thing thing})
  (.subscribe (sns-client client) (create-topic! client topic) (name protocol) thing))

(defnk subscribe-queue-to-topic!
  [sns-client :- SNSClientT
   sqs-client :- AmazonSQSClient
   topic-arn  :- s/Str
   q-url      :- s/Str
   q-arn      :- s/Str]
  (let [subscribe-result (subscribe-to-topic! {:client sns-client :topic topic-arn :protocol :sqs :thing q-arn})]
    {:subscribe subscribe-result
     :permissions (let [policy {"Version" "2012-10-17"
                                "Id" (str q-arn "/SQSDefaultPolicy")
                                "Statement" [{"Effect" "Allow"
                                              "Principal" {"AWS" "*"}
                                              "Action" "SQS:SendMessage"
                                              "Resource" q-arn
                                              "Condition" {"ArnEquals" {"aws:SourceArn" topic-arn}}}]}]
                    (log/info "Configuring SQS Policy to allow SNS subscription"
                              {:topic topic-arn :queue q-arn :policy (json/generate-string policy)})
                    (str (.setQueueAttributes sqs-client q-url {"Policy" (json/generate-string policy)})))
     :raw-delivery (.setSubscriptionAttributes sns-client (.getSubscriptionArn subscribe-result) "RawMessageDelivery" "true")}))

(defnk subscribe-queues-to-topic!
  "Creates topic, queues, dlqs"
  [sns-cli      :- SNSClientT
   sqs-cli      :- SQSConnPool
   topic        :- s/Str
   queues       :- [[s/Str s/Str]]
   {max-retries :- s/Int 5}]
  (let [sqs-client (:client sqs-cli)]
    (forv [[q dlq] queues]
          (let [topic-arn (create-topic! sns-cli topic)
                q-url   (sqs/create-queue! sqs-client q)
                q-arn   (sqs/queue-arn sqs-client q)
                dlq-url (when dlq (sqs/create-queue! sqs-client dlq))
                dlq-arn (when dlq (sqs/create-queue! sqs-client dlq))
                dlq-setup (when (and q-arn dlq-arn)
                            (sqs/enable-dead-letter-queue! sqs-client q-url dlq-arn max-retries))
                sub-result (subscribe-queue-to-topic! {:sns-client sns-cli
                                                       :sqs-client (:client sqs-cli)
                                                       :topic-arn topic-arn
                                                       :q-arn q-arn
                                                       :q-url q-url})]
            {:sqs.queue/name q
             :sqs.queue/arn  q-arn
             :sqs.queue/url  q-url
             :sqs.dlq/name dlq
             :sqs.dlq/arn dlq-arn
             :sqs.dlq/url dlq-url
             :sqs.dlq/setup dlq-setup
             :sns.topic/name topic
             :sns.topic/arn topic-arn
             :sns.topic/subscribe sub-result}))))

(comment

  (def sys @curiosity.following.web.core/sys)

  (def sns-client* (:sns-client sys))
  (def sqs-client* (-> sys :sqs-conn-pool-events-archival :client))

  (subscribe-queue-to-topic!
   {:sns-client sns-client*
    :sqs-client sqs-client*
    :topic-arn (->> sys :sns-topic-events :topic (create-topic! sns-client*))
    :q-url (->> sys :sqs-conn-pool-events-archival :queue-name (sqs/create-queue! sqs-client*))
    :q-arn (->> sys :sqs-conn-pool-events-archival
                :queue-name (sqs/create-queue! sqs-client*) (sqs/queue-arn sqs-client*))})

  )


(s/defrecord SNSTopic
    [sns sqs topic queues-csv queues results]
  component/Lifecycle
  (start [this]
    (if (some? results)
      this
      (let [queues (if (string? queues-csv)
                     (->> (str/split queues-csv #",")
                          (map vector))
                     [])
            results (subscribe-queues-to-topic! {:sns-cli sns
                                                 :sqs-cli sqs
                                                 :topic topic
                                                 :queues queues})]
        (assoc this :queues queues :results results))))
  (stop [this] this))
