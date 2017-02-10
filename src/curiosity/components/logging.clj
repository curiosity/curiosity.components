(ns curiosity.components.logging
  "Basic logging component using timbre.
  NOTE: You probably want to require this namespace prior to everything else."
  (:require [com.stuartsierra.component :as component]
            [schema.core :as s]
            [taoensso.timbre :as timbre]))

;; setup a default error level
(timbre/info "Setting default Timbre :level to :error.")
(timbre/set-level! :error)

(defrecord TimbreLogging
    [level config]
  component/Lifecycle
  (start [this]
    (do
      (timbre/set-level! (or level :info))
      (timbre/handle-uncaught-jvm-exceptions!)
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

