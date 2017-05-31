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
            [curiosity.components.aws :as aws])
  (:import com.amazonaws.services.sns.AmazonSNSClient
           com.amazonaws.auth.DefaultAWSCredentialsProviderChain
           com.amazonaws.auth.AWSCredentials
           com.amazonaws.ClientConfiguration
           curiosity.components.sqs.SQSConnPool
           curiosity.components.aws.AWSClientConfig
           curiosity.components.aws.Credentials))

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
     config :- (s/maybe AWSClientConfig)
     client :- AmazonSNSClient]
  component/Lifecycle
  (start [this]
    (assoc this :client (new-client (:creds creds) (:config config))))
  (stop [this]
    (assoc this :client nil)))

(def SNSClientT (s/if record? SNSClient AmazonSNSClient))

(s/defn sns-client :- AmazonSNSClient
  "Return an AmazonSNSClient given our component or an actual AmazonSNSClient"
  [obj :- SNSClientT]
  (if (record? obj)
    (:client obj)
    obj))

(s/defn create-topic! :- (s/maybe s/Str)
  "Idempotently create and return a topic ARN"
  [client :- SNSClientT
   topic  :- s/Str]
  (if (aws/arn? topic)
    topic
    (some-> (.createTopic (sns-client client) topic)
            .getTopicArn)))

(defnk sns-publish! :- (s/maybe s/Str)
  "Synchronously publish a message to AWS SNS; supports retries"
  [client       :- SNSClientT
   topic        :- s/Str
   message      :- s/Any
   {serializer  :- types/Fn codecs/json-dumps}
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
   {serializer      :- types/Fn codecs/json-dumps}
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
  (.subscribe (sns-client client) (create-topic! client topic) (name protocol) thing))

(defnk subscribe-queues-to-topic!
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
                sub-result (subscribe-to-topic! sns-cli topic-arn :sqs q-arn)]
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
