(ns curiosity.components.es
  (:require [com.stuartsierra.component :as component]
            [schema.core :as s]
            [plumbing.core :refer :all]
            [clojurewerkz.elastisch.rest :as esr]
            [clj-http.conn-mgr :as conn-mgr]))

(s/defrecord ElasticSearch
  [es-uri :- s/Str
   client :- s/Any
   index-name :- s/Str
   doc-type :- s/Str
   shard-count :- s/Int
   replica-cout :- s/Int
   connection-manager :- s/Any]

  component/Lifecycle
  (start [this]
    (if (nil? connection-manager)
      (assoc this :client (esr/connect es-uri))
      (assoc this :client {:connection-manager connection-manager})))
  (stop [this]
    (assoc this :client nil)))

(defnk new-elasticsearch-client
  [es-uri :- s/Str
   index-name :- s/Str
   doc-type :- s/Str
   shard-count :- s/Int
   replica-count :- s/Int]
  (map->ElasticSearch {:es-uri es-uri
                       :index-name index-name
                       :doc-type doc-type
                       :shard-count shard-count
                       :replica-count replica-count}))

(defnk new-connection-manager
  [{timeout :- s/Int 10}
   {threads :- s/Int 20}]
  (conn-mgr/make-reusable-conn-manager {:timeout timeout :threads threads}))
