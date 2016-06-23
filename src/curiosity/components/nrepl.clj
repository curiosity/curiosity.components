(ns curiosity.components.nrepl
  (:require [cider.nrepl :refer [cider-nrepl-handler]]
            [clojure.tools.nrepl.server :as nrepl-server]
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
      (assoc this :instance (nrepl-server/start-server :port port :handler cider-nrepl-handler))))
  (stop [this]
    ;; Don't do anything. We can't stop nrepl because that's how we run commands.
    this))

(s/defn new-nrepl-server
  ([]
   (map->nReplServer {}))
  ([port :- s/Int]
   (map->nReplServer {:port port})))
