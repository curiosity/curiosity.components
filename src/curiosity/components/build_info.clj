(ns curiosity.components.build-info
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str])
  (:import java.text.SimpleDateFormat
           java.util.TimeZone
           java.util.Date))

(defn map-keys
  [f m]
  (->> (map (fn [[k v]] [(f k) v]) m)
       (into {})))

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
  (-> (doto (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        (.setTimeZone (TimeZone/getTimeZone "UTC")))
      (.format (Date.))))

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
  `(spit ~path ~(build-info)))

(def BUILD_INFO_PATH "src/build_info.edn")
(def BUILD_INFO_RESOURCE_PATH "build_info.edn")

(def with-lein? #(contains? (System/getenv) "LEIN_VERSION"))

(defn read-build-info-from-edn
  [path]
  (-> path io/resource slurp read-string))

(if (with-lein?)
  (do
    (write-build-info BUILD_INFO_PATH)
    (def read-build-info build-info))
  (def read-build-info
    ;; close over the value so we don't lose if if our jar is swapped from under us
    (let [data (->> (read-build-info-from-edn BUILD_INFO_RESOURCE_PATH)
                    (map-keys keyword))]
      (fn [] data))))


(comment

  (defn read-build-info [] {:sha "fake-sha"
                            :branch "fake-branch"
                            :project-version "1.0.0-SNAPSHOT"
                            :timestamp "fake-timestamp"})


  )
