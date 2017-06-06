(ns curiosity.components.codecs
  (:refer-clojure :exclude [load])
  (:require [taoensso.nippy :as nippy]
            [cheshire.core :as json]
            [clojure.data.codec.base64 :as b64]
            [clj-time.core :as t]
            clj-time.instant))

(def nippy-dump nippy/freeze)
(defn nippy-dumps
  "Takes anything and serializes it with nippy, then base64 encodes the result and returns as a UTF-8 String"
  [x]
  (-> x
      nippy/freeze
      b64/encode
      (String. "UTF-8")))

(defn json-dumps
  "Takes anything and serializes it into JSON
  Note: JSON is far more restrictive than nippy/edn and so certain things (like non-string keys) are not
  properly supported."
  [x]
  (json/generate-string x))

(defn b64-json-dumps
  "Takes anything and serializes it into JSON, then encodes using base64 before returning a utf-8 string.
  Note: JSON is far more restrictive than nippy/edn and so certain things (like non-string keys) are not
  properly supported."
  [x]
  (-> x
      json/generate-string
      .getBytes
      b64/encode
      (String. "UTF-8")))

(def nippy-load nippy/thaw)
(defn nippy-loads
  "Takes a string and decodes it using base64, deserializing the result with nippy"
  [x]
  (-> x
      .getBytes
      b64/decode
      nippy/thaw))

(defn json-loads
  "Takes a string, deserializing with cheshire.core/parse-json"
  [x]
  (json/parse-string x))

(defn b64-json-loads
  "Takes a base64-encoded json blob, decodes it and deserializes using cheshire.core/parse-json"
  [x]
  (-> x
      .getBytes
      b64/decode
      (String. "UTF-8")
      json/parse-string))


;; our default serialization is nippy
(def dump nippy-dump)
(def load nippy-load)

;; our default string serialization is base64-encoded nippy
(def dumps nippy-dumps)
(def loads nippy-loads)

(comment

  (require '[clojure.test :refer [testing is]])
  (testing "nippy"
    (let [msg {"a" "b", "1" 2, "3" {"4" 5}}]
      (testing "nippy"
        (is (= msg (-> msg nippy-dumps nippy-loads))))
      (testing "json"
        (is (= msg (-> msg json-dumps json-loads))))
      (testing "b64-json"
        (is (= msg (-> msg b64-json-dumps b64-json-loads))))))

  

  )
