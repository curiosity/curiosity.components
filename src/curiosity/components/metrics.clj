(ns curiosity.components.metrics
  (:require [metrics.core :refer [new-registry]]
            [metrics.jvm.core :refer [instrument-jvm]]))

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
    (if registry
      (dissoc this :registry)
      this)))

(defn new-metrics []
  (map->Metrics {}))
