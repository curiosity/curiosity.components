(ns curiosity.components.codecs
  (:require [taoensso.nippy :as nippy]
            [clojure.data.codec.base64 :as b64]))

(def dump nippy/freeze)
(defn dumps
  "Takes anything and serializes it with nippy, then base64 encodes the result and returns as a UTF-8 String"
  [x]
  (-> x
      nippy/freeze
      b64/encode
      (String. "UTF-8")))

(def load nippy/thaw)
(defn loads
  "Takes a string and decodes it using base64, deserializing the result with nippy"
  [x]
  (-> x
      .getBytes
      b64/decode
      nippy/thaw))
