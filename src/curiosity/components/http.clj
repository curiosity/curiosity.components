(ns curiosity.components.http
  (:require [com.stuartsierra.component :as component]
            [ring.adapter.undertow :refer [run-undertow]]
            [schema.core :as s]
            [plumbing.core :refer :all]
            [raven-clj.ring :refer [wrap-sentry]]
            [curiosity.components.types :as types]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.nested-params :refer [wrap-nested-params]])
  (:import [curiosity.components.jdbc PooledJDBC]))


(defmacro definject-wrapper
  "Creates a ring middleware to wrap requests by injecting the specified
   resource into the request."
  [key doc]
  `(defn  ~(symbol (str "wrap-" key))
         ~doc
         [handler# thing#]
         (fn [req#]
           (-> req#
               (assoc ~(keyword key) thing#)
               handler#))))

(definject-wrapper db
  "Injects a db instance into the request")


(s/defrecord RingHandler
  [handler    :- types/Fn
   db         :- PooledJDBC
   app        :- types/Fn
   sentry-dsn :- s/Str]

  component/Lifecycle
  (start [this]
    (let [app (-> handler (wrap-db db))]
      (assoc this :app
        (if sentry-dsn
          (-> app (wrap-sentry sentry-dsn))
          app))))
  (stop [this] (dissoc this :app)))


(comment

  (use 'clojure.tools.trace)
  (trace-ns 'org.httpkit.server)
  (require '[com.stuartsierra.component :as component])
  (require '[curiosity.analytics.web.system :as system])
  (def sys (system/create-system))
  (def nsys (component/start-system sys))

  )

(defnk new-ring-handler
  "Creates a RingHandler component that will inject the postgres and redis"
  [handler    :- s/Any
   {sentry-dsn :- (s/maybe s/Str) nil}]
  (map->RingHandler {:handler handler :sentry-dsn sentry-dsn}))

(s/defrecord WebServer
  [server         :- types/Fn
   ip             :- s/Str
   port           :- s/Int
   app            :- RingHandler
   io-threads     :- s/Int
   worker-threads :- s/Int
   dispatch?      :- s/Bool]

  component/Lifecycle
  (start [this]
    (when server (.stop server))
    (let [nserver (run-undertow (:app app)
                                      {:port port
                                       :host ip
                                       :io-threads io-threads
                                       :worker-threads worker-threads
                                       :dispatch? dispatch?})]
      (assoc this :server nserver)))
  (stop [this]
    (when server
       (.stop server))
    (dissoc this :server)))

(defnk new-web-server
  "Creates an HTTP-Kit component with an injectable ring handler"
  [ip              :- s/Str
   port            :- s/Int
   {io-threads     :- (s/maybe s/Int) nil}
   {worker-threads :- (s/maybe s/Int) nil}
   {dispatch?      :- s/Bool          true}]
  (map->WebServer (merge {:ip ip :port port :dispatch? dispatch?}
                         (remove (comp nil? second)
                                 {:io-threads io-threads
                                  :worker-threads worker-threads}))))
