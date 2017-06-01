(ns curiosity.components.aws
  (:use plumbing.core)
  (:require [schema.core :as s]
            [com.stuartsierra.component :as component])
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.auth.DefaultAWSCredentialsProviderChain
           com.amazonaws.auth.AWSCredentials
           com.amazonaws.ClientConfiguration
           com.amazonaws.PredefinedClientConfigurations))

(defn arn?
  "true if s is an arn"
  [s]
  (and (string? s)
       (.startsWith s "arn:")))

(defn url?
  "true if s is a url"
  [s]
  (and (string? s)
       (.startsWith s "http:")))

(defnk new-client-config
  [{max-connections :- s/Int 1000}
   {max-error-retry :- s/Int 5}]
  (cond-> (PredefinedClientConfigurations/defaultConfig)
    (some? max-connections) (.withMaxConnections max-connections)
    (some? max-error-retry) (.withMaxErrorRetry max-error-retry)))

(s/defrecord AWSClientConfig
    [config     :- ClientConfiguration
     config-map :- {s/Keyword s/Any}]
  component/Lifecycle
  (start [this]
    (if config
      this
      (assoc this :config (new-client-config config-map))))
  (stop [this]
    (assoc this :config nil)))

(s/defrecord Credentials
    [aws-access-key :- s/Str
     aws-secret-key :- s/Str
     creds          :- AWSCredentials]
  component/Lifecycle
  (start [this]
    (if creds
      this
      (assoc this :creds
             (if (and aws-access-key aws-secret-key)
               (BasicAWSCredentials. aws-access-key aws-secret-key)
               (.getCredentials (DefaultAWSCredentialsProviderChain/getInstance))))))
  (stop [this]
    (assoc this :creds nil)))
