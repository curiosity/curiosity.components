(ns curiosity.components.metrics
  "Metrics and HealthChecks components as well as a multimethod, `health-checks`
   to implement for components who wish to provide health-checks.

  If HealthChecks is used as a component, you might be interested in
  `collect-health-checks-from-system` as a way to collect the health-checks for
  all system components prior to starting the system.
  "
  (:require [metrics.core :refer [new-registry metric-name]]
            [metrics.jvm.core :refer [instrument-jvm]]
            [metrics.health.core :as health]
            [com.stuartsierra.component :as component]
            [schema.core :as s])
  (:import clojure.lang.IFn
           [com.codahale.metrics.health HealthCheck HealthCheck$Result HealthCheckRegistry]))

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

(defn as-health-check
  "Takes a thunk and turns it into a health-check.
  Healthy if thunk is truthy.
  Any non-truthy result (or exception) is unhealthy"
  [^IFn thunk]
  (proxy [HealthCheck] []
      (check []
             (let [result (thunk)]
               (cond
                 (instance? HealthCheck$Result result) result
                 result (health/healthy)
                 :else (health/unhealthy (str "Bad (falsey) HealthCheck result: " result)))))))

(def HealthCheckThunk
  {:title s/Str
   :thunk (s/pred fn?)})

(s/defrecord HealthChecks
    [registry :- HealthCheckRegistry
     checks   :- [HealthCheckThunk]]
  component/Lifecycle
  (start [this]
    (if registry
      this
      (let [hcr (HealthCheckRegistry.)]
        (doseq [check checks]
          (.register hcr
                     (metric-name (:title check))
                     (as-health-check (:thunk check))))
        (assoc this :registry hcr))))
  (stop [this]
    (assoc this :registry
           (when registry
             (doseq [name (.getNames registry)]
                 (.unregister registry name))))))

(defn new-health-checks
  ([]
   (new-health-checks nil))
  ([checks]
   (map->Metrics {:checks checks})))

(defmulti health-checks
  "Return a sequence of HealthCheckThunks"
  class)

;; Default method for empty health checks if the object isn't supported
(defmethod health-checks ::default
  [_]
  nil)

(defn collect-health-checks-from-system
  "Walk the system's values to find
  Call this after building--but before starting--your SystemMap"
  [system health-checks-key health-checks-component]
  (->> (vals system)
       (mapcat health-checks)
       (assoc health-checks-component :checks)
       (assoc system health-checks-key)))
