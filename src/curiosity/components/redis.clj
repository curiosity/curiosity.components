(ns curiosity.components.redis
  (:require [com.stuartsierra.component :as component]
            [schema.core :as s]
            [taoensso.carmine :as car]
            [plumbing.core :refer :all]))

(s/defrecord PooledCarmine
  [redis-uri :- s/Str]

  component/Lifecycle
  (start [this]
    ;; we reuse the pool if we have one when we start
    ;; it's with-redis's responsibility to maintain this
    (assoc this :conn-opts {:pool {}
                            :spec {:uri redis-uri}}))
  (stop [this]
    (dissoc this :conn-opts)))

(defnk new-pooled-redis
  [redis-uri :- s/Str]
  (map->PooledCarmine {:redis-uri redis-uri}))

(defmacro with-redis
  "Given a redis component and some commands, run commands in the context of the redis component's conn"
  {:arglists '([redis-component :as-pipeline & body] [redis-component & body])}
  [redis-component & sigs]
  `(car/wcar ~(:conn-opts redis-component) ~@body))
