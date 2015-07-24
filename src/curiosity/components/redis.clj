(ns curiosity.analytics.lib.components.redis
  (:require [com.stuartsierra.component :as component]
            [schema.core :as s]
            [plumbing.core :refer :all]))

(s/defrecord PooledCarmine
  [redis-uri :- s/Str]

  component/Lifecycle
  (start [this]
    (assoc this :conn-opts {:pool {}
                            :spec {:uri redis-uri}}))
  (stop [this]
    (dissoc this :conn-opts)))

(defnk new-pooled-redis
  [redis-uri :- s/Str]
  (map->PooledCarmine {:redis-uri redis-uri}))
