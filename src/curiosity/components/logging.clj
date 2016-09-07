(ns curiosity.components.logging
  (:require [com.stuartsierra.component :as component]
            [schema.core :as s]
            [taoensso.timbre :as timbre]))


(defrecord TimbreLogging
    [level config]
  component/Lifecycle
  (start [this]
    (do
      (timbre/set-level! (or level :info))
      this))
  (stop [this] this))

(defn new-timbre-logging-config
  [config]
  (TimbreLogging. nil config))

(defn new-timbre-logging-level
  [level]
  (TimbreLogging. level nil))

(defn new-timbre-logging
  []
  (TimbreLogging. nil nil))




