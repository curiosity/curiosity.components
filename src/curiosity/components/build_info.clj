(ns curiosity.components.build-info
  (:require [cheshire.core :as json]
            [clj-time.core :as t]
            [clj-time.format :as tf]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [plumbing.core :refer [map-keys]]))

;; shell utility library
(def exec-str
  "Executes a string in a subshell."
  (comp (partial apply shell/sh) #(str/split % #" ")))

(defmacro list+
  "Returns a list with symbols in scope resolved and symbols
   not in scope as strings."
  [& forms]
  (->> forms
       (map (fn [x] (if (and (symbol? x)
                             (not (contains? &env x))
                             (not (resolve x)))
                      (name x)
                      x)))
       (cons `list)))

(defmacro sh
  "Now witness the firepower of this fully ARMED and OPERATIONAL battle station!"
  [& args]
  `(some->> (list+ ~@args)
            (str/join " ")
            exec-str
            :out))

;; git library
(defmacro git
  "Executes git commands. The full power of git in your repl"
  [& args]
  `(-> (sh "git" ~@args)
       str/trimr))

(defn now-iso-str
  []
  (tf/unparse (tf/formatters :date-time) (t/now)))

;; This isn't good enough if we want to try to resolve dependency issues. For that, we'll need to
;; depend on leiningen so we can let it merge it maps with its own logic.
(defn project-map
  ([]
   (project-map "project.clj"))
  ([project-file]
   (let [project-flat-map (->> project-file slurp read-string (drop 1))
         project-name (take 1 project-flat-map)
         project-version (->> (drop 1 project-flat-map) first)]
     {:data (->> project-flat-map
                        (partition 2)
                        (map vec)
                        (into {}))
      :name (str project-name)
      :version project-version})))

(defn build-info []
  {:sha (git rev-parse --short HEAD)
   :branch (git rev-parse --abbrev-ref HEAD)
   :project-version (:version (project-map))
   :timestamp (now-iso-str)})

(defmacro write-build-info
  [path]
  `(with-open [out# (io/writer ~path)]
     (json/generate-stream ~(build-info) out#)))

(def BUILD_INFO_PATH "src/build_info.json")
(def BUILD_INFO_RESOURCE_PATH "build_info.json")

(def with-lein? #(contains? (System/getenv) "LEIN_VERSION"))

(defn read-build-info-from-json
  [path]
  (with-open [in (-> path io/resource io/reader)]
    (json/parse-stream in)))

(if (with-lein?)
  (do
    (write-build-info BUILD_INFO_PATH)
    (def read-build-info build-info))
  (def read-build-info
    (memoize (comp (partial map-keys keyword)
                   (partial read-build-info-from-json BUILD_INFO_RESOURCE_PATH)))))


(comment

  (defn read-build-info [] {:sha "fake-sha"
                            :branch "fake-branch"
                            :project-version "1.0.0-SNAPSHOT"
                            :timestamp "fake-timestamp"})


  )
