(ns curiosity.components.zookeeper
  (:require [com.stuartsierra.component :as component]
            [plumbing.core :refer [defnk]]
            [schema.core :as s]
            [curator.framework :refer [curator-framework]]
            [taoensso.timbre :as log])
  (:import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2
           org.apache.curator.framework.CuratorFramework
           java.util.concurrent.TimeUnit))

;; CuratorFramework cannot implement CuratorFramework due
;; to CuratorFramework containing a start() method which conflicts
;; with component/Lifecycle T_T
(s/defrecord Curator
    [client    :- CuratorFramework
     host      :- s/Str
     port      :- s/Int
     namespace :- s/Str]
  component/Lifecycle
  (start [this]
    (let [client* (curator-framework (str host ":" port) :namespace namespace)]
      (.start client*)
      (assoc this :client client*)))
  (stop [this]
    (.stop client)
    (assoc this :client nil)))

(defn new-curator
  ([]
   (->Curator nil nil nil nil))
  ([namespace]
   (->Curator nil nil nil namespace))
  ([host port]
   (->Curator nil host port "curiosity.following"))
  ([host port namespace]
   (->Curator nil host port namespace)))

(defn curator-counting-semaphore
  [curator path max-leases]
  (InterProcessSemaphoreV2. (:client curator) path (int max-leases)))

(s/defrecord CountingSemaphore
    [curator         :- Curator
     semaphore       :- InterProcessSemaphoreV2
     path            :- s/Str
     max-leases      :- s/Str
     default-timeout :- [s/Int TimeUnit]]
  component/Lifecycle
  (start [this]
    (assoc this :semaphore (curator-counting-semaphore curator path max-leases)))
  (stop [this]
    (assoc this :semaphore nil)))

(s/defn new-counting-semaphore
  ([]
   (->CountingSemaphore nil nil nil nil [100 TimeUnit/MILLISECONDS]))
  ([path            :- s/Str
    max-leases      :- s/Int
    default-timeout :- [s/Int TimeUnit]]
   (->CountingSemaphore nil nil path max-leases default-timeout)))

(def counting-semaphore?
  "logical true if semaphore has been started"
  :semaphore)

(defmacro with-lease
  "Acquires a lease from semaphore and runs body inside a try/finally block that will close the lease.
  Accepts a CountingSemaphore with an optional :timeout of schema [s/Int TimeUnit] to override :default-timeout"
  [semaphore & body]
  `(let [lease# (when (counting-semaphore? ~semaphore)
                  (.acquire (:semaphore ~semaphore)
                            (or (-> ~semaphore :timeout first) (-> ~semaphore :default-timeout first))
                            (or (-> ~semaphore :timeout second) (-> ~semaphore :default-timeout second))))]
     (if lease#
       (do
         (log/debug "Acquired a lease." {:lease lease#
                                         :semaphore ~semaphore})
         (try
           (do ~@body)
           (finally
             (log/debug "Returning Lease. " {:lease lease#
                                             :semaphore ~semaphore})
             (.returnLease ~semaphore lease#))))
       (log/debug "Could not acquire lease!" {:lease lease#
                                              :semaphore ~semaphore}))))


(comment

  (def curator (-> (new-curator "127.0.0.1" 2181) component/start))

  (def ccs (curator-counting-semaphore curator "/brandons/sports-team" 5))

  (def l1 (.acquire ccs 100 TimeUnit/MILLISECONDS))
  (def l2 (.acquire ccs 100 TimeUnit/MILLISECONDS))
  (def l3 (.acquire ccs 100 TimeUnit/MILLISECONDS))
  (def l4 (.acquire ccs 100 TimeUnit/MILLISECONDS))

  (def l5 (.acquire ccs 100 TimeUnit/MILLISECONDS))
  (.returnLease ccs l5)

  (with-lease ccs
    (prn "sweet! have a lease"))

  (def l6 (.acquire ccs 10000 TimeUnit/MILLISECONDS))
  (= l6 nil)

  (.returnAll ccs l1 l2 l3 l4)

  )
