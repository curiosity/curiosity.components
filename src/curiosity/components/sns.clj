(ns curiosity.components.sns
  (:use plumbing.core)
  (:require [schema.core :as s]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [cheshire.core :as json]
            [clojure.core.async.impl.protocols :refer [WritePort ReadPort]]
            [curiosity.components.types :as types]
            [taoensso.timbre :as log]
            [curiosity.components.codecs :as codecs])
  (:import com.amazonaws.services.sns.AmazonSNSClient
           com.amazonaws.auth.AWSCredentials
           com.amazonaws.auth.DefaultAWSCredentialsProviderChain
           com.amazonaws.ClientConfiguration
           com.amazonaws.PredefinedClientConfigurations))

;; TODO: support everything
(defnk new-client-config
  [{max-connections :- s/Int 1000}
   {max-error-retry :- s/Int 5}]
  (cond-> (PredefinedClientConfigurations/defaultConfig)
    (some? max-connections) (.withMaxConnections max-connections)
    (some? max-error-retry) (.withMaxErrorRetry max-error-retry)))

(defrecord AWSClientConfig [config config-map]
  component/Lifecycle
  (start [this]
    (assoc this :config (new-client-config config-map)))
  (stop [this]
    (assoc this :config nil)))

(s/defn new-client :- AmazonSNSClient
  ([]
   (new-client (.getCredentials (DefaultAWSCredentialsProviderChain/getInstance))))
  ([creds :- AWSCredentials]
   (AmazonSNSClient. creds (new-client-config {})))
  ([creds  :- AWSCredentials
    config :- ClientConfiguration]
   (AmazonSNSClient. creds config)))

(defrecord SNSClient [creds config client]
  component/Lifecycle
  (start [this]
    (assoc this :client (new-client creds config)))
  (stop [this]
    (assoc this :client nil)))

(def SNSClientT (s/if record? SNSClient AmazonSNSClient))

(s/defn sns-client :- AmazonSNSClient
  "Return an AmazonSNSClient given our component or an actual AmazonSNSClient"
  [obj :- SNSClientT]
  (if (record? obj)
    (:client obj)
    obj))

(s/defn new-topic :- (s/maybe s/Str)
  "Idempotently create and return a topic ARN"
  [client :- SNSClientT
   topic  :- s/Str]
  (some-> (.createTopic (sns-client client) topic)
          .getTopicArn))

(defnk sns-publish :- (s/maybe s/Str)
  "Synchronously publish a message to AWS SNS; supports retries"
  [client       :- SNSClientT
   topic        :- s/Str
   message      :- s/Any
   {max-retries :- s/Int 5}
   {serializer  :- types/Fn codecs/json-dumps}
   {label       :- s/Str "anon-sync"}]
  (let [cli (sns-client client)
        r
        (loop [retries 0]
          (if-let [result
                   (try
                     (->> (serializer message)
                          (.publish cli topic))
                     (catch Exception e
                       (log/info "caught exception publishing sns message"
                                 {:topic topic
                                  :message message
                                  :e e
                                  :retries retries
                                  :max-retries max-retries})))]
            result
            (if (>= max-retries retries)
              nil
              (recur (inc retries)))))]
    (when r
      (if (log/may-log? :trace)
        (log/trace "Published a message: " {:msg message, :result r, :label label})
        (log/debug "Published a message: " {:msg-id (.getMessageId r), :label label}))
      (.getMessageId r))))

(defnk sns-publisher :- {:msgs (s/protocol WritePort)
                         :stop (s/protocol WritePort)
                         :unit (s/protocol ReadPort)}
  [client           :- SNSClientT
   topic            :- s/Str
   {max-buffer-size :- s/Int 10}
   {max-retries     :- s/Int 5}
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
                  (sns-publish {:client      cli
                                :topic       topic
                                :message     message
                                :max-retries max-retries
                                :serializer  serializer
                                :label       label})
                  (recur))))))]
    {:msgs messages
     :stop stop-chan
     :unit results}))
