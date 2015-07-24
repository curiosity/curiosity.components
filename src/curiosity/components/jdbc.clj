(ns curiosity.components.jdbc
  (:require [com.stuartsierra.component :as component]
            [clojure.string :as str]
            [schema.core :as s]
            [curiosity.components.types :as types]
            [plumbing.core :refer :all])
  (:import [com.mchange.v2.c3p0 ComboPooledDataSource DataSources]
           [java.net URI]))

(s/defn url->pg-spec
  "Given a Database URL for a PG DB, return a db spec"
  [url :- s/Str]
  (let [db-uri (URI. url)
        user-info (or (.getUserInfo db-uri) "")
        [user pass] (str/split user-info #":")]
    {:classname "org.postgresql.Driver"
     :subprotocol "postgresql"
     :user user
     :password pass
     :subname (if (= -1 (.getPort db-uri))
                (format "//%s%s" (.getHost db-uri) (.getPath db-uri))
                (format "//%s:%s%s" (.getHost db-uri) (.getPort db-uri) (.getPath db-uri)))}))


(s/defn c3p0-pool
  "Creates a new c3p0-managed jdbc connection pool"
  [spec :- types/Map]
  {:datasource (doto (ComboPooledDataSource.)
                 (.setDriverClass (:classname spec))
                 (.setJdbcUrl (format "jdbc:%s:%s" (:subprotocol spec) (:subname spec)))
                 (.setUser (:user spec))
                 (.setPassword (:password spec))
                 (.setMaxIdleTimeExcessConnections (* 30 60))
                 (.setMaxIdleTime (* 3 60 60)))})

(s/defrecord PooledJDBC
  [db-spec :- types/Map
   connection :- types/Map]

  component/Lifecycle
  (start [this]
    (assoc this :connection (assoc db-spec :connection (c3p0-pool db-spec))))
  (stop [this]
    (DataSources/destroy (:datasource connection))
    (dissoc this :connection)))

(defnk new-pooled-jdbc-spec
  "Creates a new c3p0-pooled JDBC Connection from db-spec"
  [db-spec :- types/Map]
  (map->PooledJDBC {:db-spec db-spec}))


(defnk new-pooled-jdbc-uri
  "Creates a new c3p0-pooled JDBC Connection from db-uri"
  [db-uri :- s/Str]
  (new-pooled-jdbc-spec {:db-spec (url->pg-spec db-uri)}))
