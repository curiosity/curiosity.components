(ns curiosity.components.nrepl
  (:require [clojure.tools.nrepl.server :as nrepl-server]
            [com.stuartsierra.component :as component]
            [schema.core :as s]
            [plumbing.core :refer :all]))

(s/defrecord nReplServer
  [port     :- s/Int
   instance :- s/Any]

  component/Lifecycle
  (start [this]
    (if instance
      this
      (assoc this :instance (nrepl-server/start-server :port port))))
  (stop [this]
    (if instance
      (dissoc this :instance)
      this)))

(s/defn new-nrepl-server
  ([]
   (map->nReplServer {}))
  ([port :- s/Int]
   (map->nReplServer {:port port})))
