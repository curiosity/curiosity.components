(ns curiosity.components.jdbc
  (:refer-clojure :exclude [cast])
  (:require [com.stuartsierra.component :as component]
            [clojure.string :as str]
            [schema.core :as s]
            [curiosity.components.types :as types]
            [plumbing.core :refer :all]
            [clojure.set :as set]
            [curiosity.utils :refer [when-seq-let]]
            [honeysql.core :as honey :refer [call]]
            [clojure.java.jdbc :as jdbc]
            [taoensso.timbre :as log])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           java.net.URI
           java.sql.Connection
           com.codahale.metrics.MetricRegistry))

;; If you're using these components, consider requiring
;; curiosity.components.injections.jdbc to get transparent json support
;; and support for joda DateTime objects.

(def TransactionIsolationLevel (s/enum Connection/TRANSACTION_NONE
                                       Connection/TRANSACTION_READ_COMMITTED
                                       Connection/TRANSACTION_READ_UNCOMMITTED
                                       Connection/TRANSACTION_REPEATABLE_READ
                                       Connection/TRANSACTION_SERIALIZABLE))
(s/defn url->pg-spec
  "Given a Database URL for a PG DB, return a db spec"
  [url :- s/Str]
  (let [db-uri (URI. url)
        user-info (.getUserInfo db-uri)
        [user pass] (if user-info  (str/split user-info #":") "")
        port (when (not= -1 (.getPort db-uri))
               (.getPort db-uri))
        host (.getHost db-uri)
        db (subs (.getPath db-uri) 1)]
    {:classname "org.postgresql.Driver"
     :subprotocol "postgresql"
     :user user
     :password pass
     :extra {:port port
             :host host
             :db db
             :args (into {} (.getQuery db-uri))
             :data-source-class-name "org.postgresql.ds.PGSimpleDataSource"}
     :subname (if-not port
                (format "//%s/%s" host db)
                (format "//%s:%s/%s" host port db))}))


(defn new-hikari-cp-config-map
  [config-map]
  (letk [[;; common options
          {db-uri                   :- (s/maybe s/Str) nil}
          {username                 :- (s/maybe s/Str) nil}
          {password                 :- (s/maybe s/Str) nil}
          {host                     :- (s/maybe s/Str) nil}
          {port                     :- (s/maybe s/Int) nil}
          {database-name            :- (s/maybe s/Str) nil}
          {data-source-class-name   :- (s/maybe s/Str) nil}
          {driver-class-name        :- (s/maybe s/Str) nil}
          {auto-commit              :- s/Bool true}
          {connection-timeout       :- s/Int   30000} ;; ms
          {idle-timeout             :- s/Int  600000} ;; ms
          {max-lifetime             :- s/Int 1800000} ;; ms
          {maximum-pool-size        :- s/Int 10}
          {metric-registry          :- (s/maybe com.codahale.metrics.MetricRegistry) nil}
          {pool-name                :- s/Str "default-pool"}
          ;; infrequently used
          {initialization-fail-fast :- s/Bool true}
          {read-only                :- s/Bool false}
          {register-mbeans          :- s/Bool false}
          {transaction-isolation    :- (s/maybe TransactionIsolationLevel) nil}
          {leak-detection-threshold :- s/Int       0} ;; ms
          {datasource-opts          :- (s/maybe types/Map) nil}]
         config-map]
        (let [m {:db-uri db-uri
                 :data-source-class-name data-source-class-name
                 :username username
                 :password password
                 :host host
                 :port port
                 :database-name database-name
                 :driver-class-name driver-class-name
                 :auto-commit auto-commit

                 :connection-timeout connection-timeout
                 :idle-timeout idle-timeout
                 :max-lifetime max-lifetime
                 :maximum-pool-size maximum-pool-size
                 :metric-registry metric-registry
                 :pool-name pool-name
                 :initialization-fail-fast initialization-fail-fast
                 :read-only read-only
                 :register-mbeans register-mbeans
                 :transaction-isolation transaction-isolation
                 :leak-detection-threshold leak-detection-threshold
                 :datasource-opts datasource-opts}]
          (if-not db-uri
            (do (prn "no extra config")
                m)
            (let [spec (url->pg-spec db-uri)
                  extra-args (-> spec :extra :args)]
              (merge-with (fn [x y] (if x x y))
                          ;; prefer passed arguments
                          m
                          ;; defaults from db-uri kwargs
                          (select-keys extra-args (keys (dissoc m :db-uri)))
                          ;; defaults from db-uri
                          {:username (let [u (:user spec)]
                                       (when-not (empty? u)
                                         u))
                           :password (let [p (:password spec)]
                                       (when-not (empty? p)
                                         p))
                           :data-source-class-name (-> spec :extra :data-source-class-name)
                           :host (-> spec :extra :host)
                           :port (-> spec :extra :port)
                           :database-name (-> spec :extra :db)
                           :datasource-opts (select-keys extra-args
                                                         (set/difference (set (keys extra-args))
                                                                         (set (keys m))))}
                          ;; from pg environment variables
                          {:username (System/getenv "PGUSER")
                           :password (System/getenv "PGPASS")}
                          ;; default to current user like psql
                          {:username (System/getProperty "user.name")}))))))

(defn new-hikari-cp-config
  [m]
  (let [config (doto (HikariConfig.)
                 (.setUsername (:username m))
                 (.setPassword (:password m))
                 (.setAutoCommit (:auto-commit m))
                 (.setConnectionTimeout (:connection-timeout m))
                 (.setIdleTimeout (:idle-timeout m))
                 (.setMaxLifetime (:max-lifetime m))
                 (.setMaximumPoolSize (:maximum-pool-size m))
                 (.setPoolName (:pool-name m))
                 (.setInitializationFailFast (:initialization-fail-fast m))
                 (.setReadOnly (:read-only m))
                 (.setRegisterMbeans (:register-mbeans m))
                 (.setLeakDetectionThreshold (:leak-detection-threshold m)))
        datasource-opts (merge-with (fn [x y] x)
                                    (:datasource-opts m)
                                    {"ServerName" (:host m)
                                     "DatabaseName" (:database-name m)
                                     "PortNumber" (:port m)})]
    ;; handle nil-able options
    (when-let [data-source-class-name (:data-source-class-name m)]
      (.setDataSourceClassName config data-source-class-name))
    (when-let [driver-class-name (:driver-class-name m)]
      (.setDriverClassName config driver-class-name))
    (when-let [metric-registry (:metric-registry m)]
      (.setMetricRegistry config metric-registry))
    (when-let [tx-isolation-level (:transaction-isolation-level m)]
      (.setTransactionIsolationLevel config tx-isolation-level))
    (when datasource-opts
      (doseq [[k v] datasource-opts]
        (.addDataSourceProperty config k v)))
    [config (assoc m :datasource-opts datasource-opts)]))

(s/defn new-hikari-cp-pool
  [opts]
  (let [[config opts] (-> (new-hikari-cp-config-map opts)
                        new-hikari-cp-config)]
    {:datasource (HikariDataSource. config)
     :opts opts}))

(s/defrecord PooledJDBC
    [db-uri :- (s/maybe s/Str)
     opts :- (s/maybe types/Map)
     datasource :- (s/maybe com.zaxxer.hikari.HikariDataSource)]

  component/Lifecycle
  (start [this]
    (if datasource
      this
      (when (or db-uri (:jdbc-url opts))
        (let [{:keys [opts datasource]}
              (new-hikari-cp-pool (if (:db-uri opts)
                                    opts
                                    (assoc opts :db-uri db-uri)))]
          (assoc this
                 :datasource datasource
                 :opts opts)))))
  (stop [this]
    (when datasource
      (.close datasource))
    (assoc this :datasource nil)))

(s/defn new-pooled-jdbc-uri
  "Creates a new hikari-cp-pooled postgresql jdbc connection from a db-uri"
  ([]
   (map->PooledJDBC {}))
  ([db-uri :- s/Str]
   (map->PooledJDBC {:db-uri db-uri})))

(s/defn new-pooled-jdbc-opts
  "Creates a new hikari-cp-pooled jdbc connection from hikari options map"
  ([]
   (map->PooledJDBC {}))
  ([opts :- types/Map]
   (map->PooledJDBC {:opts opts})))


(defn snake-case
  "Foo-bar -> foo_bar"
  [s]
  (-> s str/lower-case (str/replace "-" "_")))

(defn kebob-case
  "Foo_Bar -> foo-bar"
  [s]
  (-> s str/lower-case (str/replace "_" "-")))

(defn cast
  "CAST()"
  [type field]
  (call :cast field type))

(def ^:dynamic *jdbc-log-level*
  "Log Level at which to print honey-jdbc-runner queries. nil is off."
  nil)

(defmacro with-query-logged
  {:arglists '[[& body]
               [log-level & body]]
   :doc "Runs the body with queries logged at log-level (default :error)"}
  [& body]
  (let [[car & cdr] body
        log-level (if (keyword? car) car :error)
        query (if (keyword? car) cdr body)]
    `(binding [~'curiosity.components.jdbc/*jdbc-log-level* ~log-level]
       (do ~@query))))

(defn query-logger
  "Helper to spy on honeysql queries at a particular level defined by *jdbc-log-level*"
  [q]
  (when-let [level *jdbc-log-level*]
    (log/spy level q))
  q)

(s/defn honey-jdbc-runner :- (s/maybe [s/Any])
  "Returns a vector of results given a db-component and a built honeysql map.
  Takes a jdbc-fn implementation and feeds the db-component and the generated sql vec from query-map to it"
  [jdbc-fn db-component query-map]
  (when-seq-let [results (->> (honey/format query-map :quoting :ansi)
                              query-logger
                              (jdbc-fn db-component))]
                (vec results)))

(def ^{:arglists '[[db-conn honeysql-map]]}
  query-runner
  "Given a db-component and a query-map, query using the query-map returning a vector or nil"
  (partial honey-jdbc-runner #(jdbc/query %1 %2 :identifiers kebob-case)))

(def ^{:arglists '[[db-conn honeysql-map]]}
  exec-runner
  "Given a db-component and a query-map, execute the query-map returning a vector or nil"
  (partial honey-jdbc-runner jdbc/execute!))

