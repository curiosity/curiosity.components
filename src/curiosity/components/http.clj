(ns curiosity.components.http
  (:require [com.stuartsierra.component :as component]
            [ring.adapter.undertow :refer [run-undertow]]
            immutant.web
            immutant.web.undertow
            [ring.middleware.format-response :refer [wrap-restful-response]]
            [schema.core :as s]
            [plumbing.core :refer :all]
            [raven-clj.ring :as raven-ring]
            [curiosity.components.types :as types]
            [slingshot.slingshot :refer [try+ throw+]]
            metrics.ring.instrument
            metrics.ring.expose))

;; If you're using this, consider requiring curiosity.components.injections.json
;; to enable transparent DateTime encoding.

(defrecord RingMiddleware
  [middleware-fn dependencies middleware]
  ;; be a component
  component/Lifecycle
  (start [this]
    (if middleware
      middleware
      (let [deps (for [d dependencies]
                   (if (var? d)
                     d
                     (get this d)))]
        (assoc this :middleware (fn [handler] (middleware-fn deps handler))))))
  (stop [this]
    (assoc this :middleware nil)))

(defn new-ring-middleware
  "Creates a RingMiddleware"
  ([middleware-fn]
   (new-ring-middleware middleware-fn []))
  ([middleware-fn dependencies]
  (map->RingMiddleware {:middleware-fn middleware-fn
                        :dependencies dependencies})))


(defn expose-metrics-at-slash-metrics
  "Exposes metrics at /json"
  [[registry-var] handler]
  ;; This get passed a var pointing to an atom, so double deref
  (metrics.ring.expose/expose-metrics-as-json handler "/metrics" @@registry-var))

(defn instrument-ring-handler
  "Instruments a ring handler with some basic metrics"
  [[registry-var] handler]
  ;; This get passed a var pointing to an atom, so double deref
  (metrics.ring.instrument/instrument handler @@registry-var))

(defn sentry-wrapper
  "Wraps a ring handler, sending 500s to sentry"
  [[sentry-dsn] handler]
  (raven-ring/wrap-sentry handler sentry-dsn))

;; Injections is a vector of keywords that will be injected on the component
;; prior to start time. At start time, they are selected off the RingHandler
;; and added to a closure applied to each request.

;; Middleware is a vector of keywords or fns to be composed together to wrap
;; the handler. If keywords, there should be a middleware wrapper at that keyword
;; on the RingHandler by dependency injection. The first middleware listed is closest
;; to the handler. The last middleware is the first middleware executed on a request and
;; the last middleware to have access to the response.
;; The schema definition [(s/either s/Keyword types/Fn)] is technically redundant
;; (keywords are IFns, but the redundancy helps document the intended shape).
(s/defrecord RingHandler
  [handler    :- types/Fn
   injections :- [s/Keyword]
   middleware :- [(s/either s/Keyword types/Fn)]
   app        :- types/Fn]

  component/Lifecycle
  (start [this]
    (let [app (fn [req]
                (let [injectables (select-keys this injections)]
                  (handler (if (empty? injectables)
                             req
                             (apply assoc req (apply concat injectables))))))
                           ;; resolve the middleware
          wrapper (->> (map #(if (keyword? %) (-> this % :middleware) %) middleware)
                           ;; reverse get correct composition order
                           reverse
                           (apply comp identity))]
      (assoc this :app (wrapper app))))
  (stop [this]
    (assoc this :app nil)))

(defnk new-ring-handler
  "Creates a RingHandler component that will inject the postgres and redis"
  [handler    :- s/Any
   {injections :- [s/Keyword] []}
   {middleware :- [(s/either s/Keyword types/Fn)] []}]
  (map->RingHandler {:handler handler
                     :injections injections
                     :middleware middleware}))

(s/defrecord WebServer
  [server             :- (s/pred map?)
   ip                 :- s/Str
   port               :- s/Int
   app                :- RingHandler
   options            :- (s/pred map?)]

  component/Lifecycle
  (start [this]
    (if server
      this
      (assoc this :server (immutant.web/run (:app app)
                            (immutant.web.undertow/options (merge {:port port
                                                          :host ip}
                                                         options))))))
  (stop [this]
    (when server
      (immutant.web/stop server))
    (assoc this :server nil)))

(defnk new-web-server
  "Creates an Undertow.io component with an injectable ring handler"
  [{port           :- s/Int           8080}
   {ip             :- s/Str           "127.0.0.1"}
   {options        :- (s/pred map?)   {}}]
  (map->WebServer {:ip ip :port port :options options}))

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

(comment

  (ns foo)
  (use 'curiosity.components.http)
  (use 'clojure.tools.trace)
  (require 'com.stuartsierra.component)
  (trace-ns 'curiosity.components.http)
  (use 'clojure.repl)

  (defn wrap-printer-middleware
    [handler n]
    (fn [req]
      (prn "printer-middleware: " n "request:" req)
      (let [res (handler req)]
        (prn "printer-middleware: " n " response: " res)
        res)))

  (defn hello-world-handler [req] {:status 200 :body "hello world"})
  (def pikachu-middleware (com.stuartsierra.component/start
                             (new-ring-middleware (fn [_ handler] (wrap-printer-middleware handler)))))
  (def rh (assoc (new-ring-handler {:handler hello-world-handler :middleware [:pikachu]})
                 :pikachu pikachu-middleware))
  (def nrh (com.stuartsierra.component/start rh))
  ((:app nrh) {:spam "eggs"})

  )

