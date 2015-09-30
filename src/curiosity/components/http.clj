(ns curiosity.components.http
  (:require [com.stuartsierra.component :as component]
            [ring.adapter.undertow :refer [run-undertow]]
            [ring.middleware.format.response :refer [wrap-restful-response]]
            [schema.core :as s]
            [plumbing.core :refer :all]
            [raven-clj.ring :as raven-ring]
            [curiosity.components.types :as types]
            [slingshot.slingshot :refer [try+]]
            metrics.ring.instrument
            metrics.ring.expose))

;; If you're using this, consider requiring curiosity.components.injections.json
;; to enable transparent DateTime encoding.

(defrecord RingMiddleware
  [middleware-fn dependencies middleware]
  component/Lifecycle
  (start [this]
    (if middleware
      middleware
      (assoc this :middleware (partial middleware-fn (select-keys this dependencies)))))
  (stop [this]
    (if middleware
      (dissoc this :middleware)
      this)))

(defn new-ring-middleware
  "Creates a RingMiddleware"
  [middleware-fn dependencies]
  (map->RingMiddleware {:middleware-fn middleware-fn
                        :dependencies dependencies}))


(defn expose-metrics-at-slash-metrics
  "Exposes metrics at /json"
  [[registry] handler]
  (metrics.ring.expose/expose-metrics-as-json handler "/metrics" registry))

(defn instrument-ring-handler
  "Instruments a ring handler with some basic metrics"
  [[registry] handler]
  (metrics.ring.instrument/instrument handler registry))

(defn sentry-wrapper
  "Wraps a ring handler, sending 500s to sentry"
  [[sentry-dsn] handler]
  (raven-ring/wrap-sentry handler sentry-dsn))

;; Injections is a vector of keywords that will be injected on the component
;; prior to start time. At start time, they are selected off the RingHandler
;; and added to a closure applied to each request.

;; Middleware is a vector of keywords or fns to be composed together to wrap
;; the handler. If keywords, there should be a middleware wrapper at that keyword
;; on the RingHandler by dependency injection.
;; The schema definition [(s/either s/Keyword types/Fn)] is technically redundant
;; (keywords are IFns, but the redundancy helps document the intended shape).
(s/defrecord RingHandler
  [handler    :- types/Fn
   injections :- [s/Keyword]
   middleware :- [(s/either s/Keyword types/Fn)]
   app        :- types/Fn
   sentry-dsn :- s/Str]

  component/Lifecycle
  (start [this]
    (let [app (fn [req]
                (let [injectables (select-keys this injections)]
                  (handler (if (empty? injectables)
                             req
                             (apply assoc req (apply concat injectables))))))

                           ;; resolve the middleware
          wrapped-app (->> (map #(if (keyword? m) (-> this m) m) middleware)
                           ;; order them correctly for composition
                           reverse
                           ;; final wrap
                           (apply comp identity))]
      (assoc this :app wrapped-app)))
  (stop [this] (dissoc this :app)))

(defnk new-ring-handler
  "Creates a RingHandler component that will inject the postgres and redis"
  [handler    :- s/Any
   {sentry-dsn :- (s/maybe s/Str) nil}
   {injections :- [s/Keyword] []}]
  (map->RingHandler {:handler handler
                     :sentry-dsn sentry-dsn
                     :injections injections}))

(defn stop-undertow!
  "Stops the undertow instance"
  [ut]
  (try
    (.stop ut)
    (catch NullPointerException _)))

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
    (if server
      this
      (let [nserver (run-undertow (:app app)
                                  {:port port
                                   :host ip
                                   :io-threads io-threads
                                   :worker-threads worker-threads
                                   :dispatch? dispatch?})]
        (assoc this :server nserver))))
  (stop [this]
    (stop-undertow! server)
    (dissoc this :server)))

(defnk new-web-server
  "Creates an HTTP-Kit component with an injectable ring handler"
  [{port           :- s/Int           8080}
   {ip             :- s/Str           "127.0.0.1"}
   {io-threads     :- (s/maybe s/Int) nil}
   {worker-threads :- (s/maybe s/Int) nil}
   {dispatch?      :- s/Bool          true}]
  (map->WebServer (merge {:ip ip :port port :dispatch? dispatch?}
                         (remove (comp nil? second)
                                 {:io-threads io-threads
                                  :worker-threads worker-threads}))))

(defn wrap-slingshot-response
  "Catches slingshot-responses and renders them appropriately"
  [handler]
  (fn [request]
    (try+
      (handler request)
      (catch [::type ::request-response]
        {response ::response}
        ;; this is a hack to use a middleware on a response
        ;; we build a "handler" that just returns response, then
        ;; call the result of the middleware (which is a handler)
        ;; with the request (so it can figure out how/if to serialize)
        ((wrap-restful-response (fn [& _] response)) request)))))

(defn slingshot-response
  "Throws a response map that short-circuits most additional processing"
  [response]
  (throw+ {::type ::request-response
           ::response response}))

(s/defn ring-response
  ([body :- s/Any]
   (ring-response 200 body))
  ([status :- s/Int body :- s/Any]
   {:status status
    :body body}))

(def ok (partial ring-response 200))
(def created (partial ring-response 201))
(def accepted (partial ring-response 202))
(def found (partial ring-response 302))
(def not-found (partial ring-response 404))
