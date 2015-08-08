(ns curiosity.components.settings
  (:require [plumbing.core :refer [map-keys fn->>]]
            [schema.core :as s]
            [schema.coerce :as coerce]
            [clojure.string :refer [replace]]
            [curiosity.utils :refer [have! get']]
            [environ.core :as environ]))

;; Yes this is called some? in 1.6+
;; No, storm does not support 1.6+
(defn not-nil?
  {:static true}
  [x]
  (not (nil? x)))

(defn validate-keys
  "Asserts that dependent is set when source is"
  [m source dependent]
  (when (get' m source)
    (have! not-nil? (get' m dependent))))

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

(defn json-coercion-matcher
  "A matcher that coerces keywords and keyword enums from strings, and longs and doubles
  from numbers on the JVM (without losing precision)"
  [schema]
  (or (+str->int-coercions+ schema)
      (coerce/+json-coercions+ schema)
      (coerce/keyword-enum-matcher schema)
      (coerce/set-matcher schema)))


(def build-settings
  "Final pass before validating the settings map"
  ;; this is where discovery would go if we performed it
  identity)

(defn env
  []
  (merge
    ;; These are technically private. I don't care.
    (#'environ/read-env-file)
    (#'environ/read-system-env)
    (#'environ/read-system-props)))

(def prefix-keys
  #(map-keys (fn->> name (str %1 "-") keyword) %2))

(defn resolve-settings!
  ([prefix schema defaults]
   (resolve-settings! prefix schema defaults nil))
  ([prefix schema defaults overrides]
   (let [coercer (coerce/coercer schema json-coercion-matcher)
         ?valid-settings (->> schema
                              (map-keys (fn->> name (str prefix "-") keyword))
                              keys
                              (select-keys (env))
                              (map-keys (fn->> name
                                               (#(replace % (re-pattern (str prefix "-")) ""))
                                               keyword))
                              (#(merge defaults % overrides))
                              build-settings)]
     (coercer ?valid-settings))))
