(ns curiosity.components.types
  (:require [schema.core :as s]))

(def Vector (class []))
(def Map (class {}))
(def Set (class #{}))
(def Fn (s/pred fn?))
