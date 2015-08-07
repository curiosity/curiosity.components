(ns curiosity.components.http
  (:require [com.stuartsierra.component :as component]
            [ring.adapter.undertow :refer [run-undertow]]
            [schema.core :as s]
            [plumbing.core :refer :all]
            [raven-clj.ring :refer [wrap-sentry]]
            [curiosity.components.types :as types]))


;; Injections is a set of keywords that will be injected on the component
;; prior to start time. At start time, they are selected off the RingHandler
;; and added to a closure applied to each request.
(s/defrecord RingHandler
  [handler    :- types/Fn
   injections :- [s/Keyword]
   app        :- types/Fn
   sentry-dsn :- s/Str]

  component/Lifecycle
  (start [this]
    (let [app #(handler (apply assoc % (select-keys this injections)))]
      (assoc this :app (if sentry-dsn (wrap-sentry app sentry-dsn) app))))
  (stop [this] (dissoc this :app)))


(defnk new-ring-handler
  "Creates a RingHandler component that will inject the postgres and redis"
  [handler    :- s/Any
   {sentry-dsn :- (s/maybe s/Str) nil
    injections :- [s/Keyword] []}]
  (map->RingHandler {:handler handler
                     :sentry-dsn sentry-dsn
                     :injections injections}))

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
