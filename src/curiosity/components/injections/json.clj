;; This file must be required very early due to monkey patching
(ns curiosity.analytics.lib.injections
  (:require [cheshire.generate :as json-generate]
            [clj-time.core :as t]
            [clj-time.format :as tfmt])
  (:import org.joda.time.DateTime))

;; setup cheshire to properly encode JodaTimes
(def date-to-json-formatter
  (tfmt/with-zone
    (tfmt/formatters :date-time)
    t/utc))

(json-generate/add-encoder
  org.joda.time.DateTime
  (fn [d json-generator]
    (.writeString json-generator
                  (tfmt/unparse date-to-json-formatter d))))
