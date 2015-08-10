(ns curiosity.components.types
  (:require [schema.core :as s]))

(def Vector (s/pred vector?))
(def Map (s/pred map?))
(def Set (s/pred set?))
(def Fn (s/pred fn?))
