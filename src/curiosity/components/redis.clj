(ns curiosity.components.redis
  (:require [com.stuartsierra.component :as component]
            [schema.core :as s]
            [taoensso.carmine :as car]
            ;; side-note: we can't remove this as long as we use Worker
            ;; since Worker isn't AOT-compiled, this has a side-effect of
            ;; compiling the carmine message-queue namespace into a class
            ;; that we can import
            [taoensso.carmine.message-queue :as car-mq]
            [slingshot.slingshot :refer [throw+]]
            [curiosity.components.types :as types]
            [plumbing.core :refer :all])
  ;; this currently requires (:require taoensso.carmine.message-queue)
  (:import taoensso.carmine.message_queue.Worker))

(s/defrecord PooledCarmine
  [redis-uri :- s/Str
   conn-opts :- types/Map]

  component/Lifecycle
  (start [this]
    ;; we reuse the pool if we have one when we start
    ;; it's with-redis's responsibility to maintain this
    (when-not redis-uri
      (throw+ {:type :improperly-configured
               :reason :redis-uri-is-required
               :this this}))
    (assoc this :conn-opts {:pool {}
                            :spec {:uri redis-uri}}))
  (stop [this]
    (dissoc this :conn-opts)))

(s/defn new-redis
  ([]
   (map->PooledCarmine {}))
  ([redis-uri :- s/Str]
   (map->PooledCarmine {:redis-uri redis-uri})))


(defmacro with-redis
  "Given a redis component and some commands, run commands in the context of the redis component's conn"
  {:arglists '([redis-component :as-pipeline & body] [redis-component & body])}
  [redis-component & sigs]
  `(car/wcar ~(:conn-opts redis-component) ~@sigs))


(s/defrecord CarmineWorker
  [queue-name :- s/Str
   threads :- s/Int
   handler :- types/Fn
   redis :- PooledCarmine
   workers :- [Worker]]

  component/Lifecycle
  (start [this]
    ;; validation
    (when-not (:conn-opts redis)
      (throw+ {:type :improperly-configured
               :reason :dependency-missing
               :dependency [:redis :conn-opts]
               :this this}))
    (when-not queue-name
      (throw+ {:type :improperly-configured
               :reason :missing-queue-name-param
               :this this}))
    (when-not (pos? threads)
      (throw+ {:type :improperly-configured
               :reason :positive-threads-required
               :this this}))
    ;; build the workers
    (if-not (empty? workers)
      this
      (loop [cnt threads
             self this]
        (if (zero? cnt)
          self
          (recur (dec cnt)
                 (assoc self :workers
                        (conj (or (:workers self) [])
                              (car-mq/worker (:conn-opts redis)
                                             queue-name
                                             {:handler handler}))))))))
    (stop [this]
      ;; stop the workers if they exist
      (when workers
        (doseq [w workers]
          (car-mq/stop w)))
      ;; always return :workers []
      (assoc this :workers [])))


(defn enqueue
  "Enqueues a message at queue-name using redis-component"
  [redis-component queue-name msg]
  (with-redis redis-component
    (car-mq/enqueue queue-name msg)))


(defnk new-worker
  "Takes a handler, with queue-name, workers, threads, and redis optional and injectable"
  [handler :- types/Fn
   {queue-name :- s/Str nil}
   {workers :- [Worker] []}
   {threads :- s/Int 1}
   {redis :- PooledCarmine nil}]
  (map->CarmineWorker {:queue-name queue-name
                       :threads threads
                       :handler handler
                       :workers workers
                       :redis redis}))


(defn simple-worker-wrapper
  "Wraps a function that receives a message and automatically acks the message unless
   an exception is thrown. If an exception is thrown, retry? true will retry the message,
   while retry? false will fail the message.

   If you need configurable behavior, including backoff/retry timeouts besides the default
   write your own handler. See taoensso.carmine.message-queue/worker's docstrings for more
   info on exact requirements."
  [retry? f]
  (fn [{:keys [mid message attempt]}]
    (try
      (f message)
      {:status :success}
      (catch Throwable t
        {:status (if retry? :retry :fail)
         :throwable t}))))
