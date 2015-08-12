(ns curiosity.components.settings
  (:require [com.stuartsierra.component :as component]
            [plumbing.core :refer [map-keys fn->>]]
            [schema.core :as s]
            [schema.coerce :as coerce]
            [schema.utils :refer [error?] :rename {error? schema-error?}]
            [slingshot.slingshot :refer [throw+]]
            [clojure.string :as str]
            [curiosity.components.types :as types]
            [environ.core :as environ]))

(defn parse-number
  "Reads a number from a string. Returns nil if not a number."
  [s]
  (if  (re-find #"^-?\d+\.?\d*([Ee]\+\d+|[Ee]-\d+|[Ee]\d+)?$"  (.trim s))
    (read-string s)))

(defn try-number-fallback-str
  [num]
  (cond
    (integer? num) num                ;; stop, we're an int
    (number? num) (int num)           ;; we can cast to an int
    :else (let [n (parse-number num)] ;; let's see if we can get a num
            (if (number? n)
              (int n)                 ;; cast to an int
              num))))                 ;; fallback to original input

(def +str->int-coercions+
  {s/Int try-number-fallback-str})

(s/defn json-coercion-matcher
  "A matcher that coerces keywords and keyword enums from strings, and longs and doubles
  from numbers on the JVM (without losing precision)"
  [schema :- types/Map]
  (or (+str->int-coercions+ schema)
      (coerce/+json-coercions+ schema)
      (coerce/keyword-enum-matcher schema)
      (coerce/set-matcher schema)))

(defn env
  []
  (merge
    ;; These are technically private. I don't care.
    (#'environ/read-env-file)
    (#'environ/read-system-env)
    (#'environ/read-system-props)))

(s/defn resolve-settings!*
  [prefix :- s/Str
   schema :- types/Map
   defaults :- types/Map
   overrides :- types/Map]
  (let [coercer (coerce/coercer schema json-coercion-matcher)
        ?valid-settings (->> schema
                             (map-keys (fn->> name (str prefix "-") keyword))
                             keys
                             (select-keys (env))
                             (map-keys (fn->> name
                                              (#(str/replace % (re-pattern (str prefix "-")) ""))
                                              keyword))
                             (#(merge defaults % overrides)))]
     (coercer ?valid-settings)))

(defn resolve-settings!
  ([prefix schema defaults]
   (resolve-settings! prefix schema defaults nil))
  ([prefix schema defaults overrides]
   (let [config (resolve-settings!* prefix schema defaults overrides)]
     (if (schema-error? config)
       (throw+ {:type :invalid-configuration
                :error-container config})
       config))))

(s/defn composite-system
  "Takes some system factories turns them into map"
  [systems :- [types/Fn]]
  (->> ((apply juxt systems))
       (apply merge)))

(s/defn create-system
  "Creates a SystemMap given a system-factory based on available settings."
  [sys-factory :- types/Fn]
  (->> (sys-factory)
       (apply concat)
       (apply component/system-map)))

(defmacro def-system-shortcuts
  "Defines create-system, start-system, stop-system, and jump-start in your namespace"
  [project-name settings-schema settings-defaults]
  `(do
     (curiosity.utils/defalias ~'create-system curiosity.components.settings/create-system)
     (curiosity.utils/defalias ~'start-system com.stuartsierra.component/start-system)
     (curiosity.utils/defalias ~'stop-system com.stuartsierra.component/stop-system)
     (defn ~'jump-start
       "Takes a system-factory (settings parameter) and returns the started system"
       [~'system-factory] (-> ~'system-factory ~'create-system ~'start-system))))
