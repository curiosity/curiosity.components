(defproject curiosity.components "0.9.3"
  :description "Curiosity.com components."
  :url "http://github.com/curiosity/curiosity.components"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-optons ["-Xmx1G"]
  :repl-options {:timeout 120000}
  :repositories [["s3" {:url "s3p://curiosity-java-jars-private/jars/"
                        :no-auth true}]]
  :plugins [[s3-wagon-private "1.3.0"]]
  :dependencies [[org.clojure/clojure "1.9.0-alpha17"]
                 [prismatic/plumbing "0.5.4"]
                 [prismatic/schema "1.1.6"]
                 [com.taoensso/encore "2.91.1"]
                 [curiosity.utils "0.9.3" :exclusions [com.taoensso/encore]]
                 [environ "1.1.0"]
                 [org.clojure/core.async "0.3.443"]
                 [com.emidln/destructured-component "0.3.0"]
                 [slingshot "0.12.2"]

                 ;;
                 ;; Make everything uses timbre for logging.
                 ;; EVERY.
                 ;; THING.
                 ;;
                 [org.clojure/tools.logging "0.4.0"]
                 [commons-codec/commons-codec "1.10"]
                 ;; all other options are insane
                 [com.taoensso/timbre "4.10.0"]
                 ;; make slf4j use timbre
                 [org.slf4j/slf4j-api "1.7.25"]
                 [com.fzakaria/slf4j-timbre "0.3.6"]
                 ;; make everything else use slf4j
                 [org.slf4j/log4j-over-slf4j "1.7.25"]
                 [org.slf4j/jul-to-slf4j "1.7.25"]
                 [org.slf4j/jcl-over-slf4j "1.7.25"]
                 ;; pin commons-logging so that it picks up slf4j correctly
                 [commons-logging "1.2"]
                 [timbre-ns-pattern-level "0.1.2"]

                 ;; serialization
                 ; DateTime
                 [joda-time/joda-time "2.9.9"]
                 [clj-time "0.13.0"]
                 ; JSON
                 [cheshire "5.7.1"]

                 ;; metrics component
                 [metrics-clojure "2.9.0"]
                 [metrics-clojure-jvm "2.9.0"]
                 [metrics-clojure-ring "2.9.0"]
                 [metrics-clojure-health "2.9.0"]

                 ;; s3, sqs, and sns
                 [com.amazonaws/aws-java-sdk "1.11.153"]

                 ; communicate with s3
                 [com.curiosity/clj-aws-s3 "0.5.0" :exclusions [joda-time com.fasterxml.jackson.core/jackson-annotations]]
                 ; communicate with sqs
                 [com.cemerick/bandalore "0.0.6" :exclusions [joda-time/joda-time]]

                 ;; elasticsearch
                 [clojurewerkz/elastisch "2.2.2"]

                 ;; redis
                 [com.curiosity/crache "1.0.0-rc3" :exclusions [com.taoensso/carmine]]
                 [com.taoensso/carmine "2.16.0"]

                 ;; postgresql
                 ;; [org.clojure/java.jdbc "0.3.6"] ;; last known-working version
                 [org.clojure/java.jdbc "0.6.1"] ;; bridge version
                 ;; [org.clojure/java.jdbc "0.7.0-SNAPSHOT"] ;; current version
                 ; postgresql driver
                 ;; [org.postgresql/postgresql "9.4.1209"] ;; last known-working version
                 [org.postgresql/postgresql "42.1.1"] ;; current version
                 ; connection pooling
                 ;;[com.zaxxer/HikariCP-java6 "2.3.13"] ;; last known-working version
                 [com.zaxxer/HikariCP "2.6.3"] ;; current-version
                 [honeysql "0.8.2"]
                 [nilenso/honeysql-postgres "0.2.2"]

                 ;; nrepl
                 [org.clojure/tools.nrepl "0.2.13"]
                 [cider/cider-nrepl "0.14.0"]

                 ;; http
                 [javax.servlet/servlet-api "2.5"]
                 [ring "1.6.1"]
                 [ring/ring-codec "1.0.1"]
                 [ring/ring-headers "0.3.0"]
                 [ring/ring-defaults "0.3.0"]
                 [ring-middleware-format "0.7.2"]
                 [jumblerg/ring.middleware.cors "1.0.1"]
                 [ring-basic-authentication "1.0.5"]
                 [org.immutant/web  "2.1.8"]
                 [io.undertow/undertow-core "1.4.12.Final"]

                 [clj-simpleflake "0.1.0"]

                 ;; http error reporting
                 [raven-clj "1.5.0"]

                 ;; zookeeper support using 3.4.x; 3.5 isn't stable; curator 3.x requires zk 3.5!
                 [org.apache.zookeeper/zookeeper "3.4.10" :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.apache.curator/curator-recipes "3.3.0"]
                 [org.apache.curator/curator-client "3.3.0"]
                 [org.apache.curator/curator-framework "3.3.0"]
                 [org.apache.curator/curator-x-discovery "3.3.0"]
                 [curator "0.0.7" :exclusions [org.apache.curator/curator-recipies
                                               org.apache.curator/curator-framework
                                               org.apache.curator/curator-x-discovery]]])
