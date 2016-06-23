(ns curiosity.components.metrics
  (:require [metrics.core :refer [new-registry]]
            [metrics.jvm.core :refer [instrument-jvm]]
            [com.stuartsierra.component :as component]))

(defrecord Metrics
  [registry]
  component/Lifecycle
  (start [this]
    (if registry
      this
      (let [r (new-registry)]
        (instrument-jvm r)
        (assoc this :registry r))))
  (stop [this]
    (assoc this :registry nil)))

(defn new-metrics []
  (map->Metrics {}))
