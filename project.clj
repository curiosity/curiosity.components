(defproject curiosity.components "0.6.0-SNAPSHOT"
  :description "Curiosity.com components. Proprietary. Do not distribute."
  :url "http://github.com/curiosity/curiosity.components"
  :license {:name "Proprietary. Do not distribute."
            :url "https://curiosity.com/"}
  :jvm-optons ["-Xmx1G"]
  :repl-options {:timeout 120000}
  :repositories [["s3" {:url "s3p://curiosity-java-jars-private/jars/"
                        :username :env/aws_access_key
                        :passphrase :env/aws_secret_key}]]
  :plugins [[s3-wagon-private "1.1.2"]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [prismatic/plumbing "0.5.3"]
                 [prismatic/schema "1.1.2"]
                 [com.taoensso/encore "2.58.0"]
                 [curiosity.utils "0.8.0" :exclusions [com.taoensso/encore]]
                 [environ "1.0.3"]
                 [org.clojure/core.async "0.2.385"]
                 [com.emidln/destructured-component "0.3.0"]
                 [slingshot "0.12.2"]
                 [org.clojure/tools.logging "0.3.1"]

                 ;; serialization
                 ; DateTime
                 [joda-time/joda-time "2.9.4"]
                 ; JSON
                 [cheshire "5.6.2"]

                 ;; metrics component
                 [metrics-clojure "2.7.0"]
                 [metrics-clojure-jvm "2.7.0"]
                 [metrics-clojure-ring "2.7.0"]

                 ;; s3 and sqs
                 [com.amazonaws/aws-java-sdk "1.11.9"]

                 ; communicate with s3
                 [com.curiosity/clj-aws-s3 "0.4.0" :exclusions [joda-time com.fasterxml.jackson.core/jackson-annotations]]
                 ; communicate with sqs
                 [com.cemerick/bandalore "0.0.6" :exclusions [joda-time/joda-time]]

                 ;; elasticsearch
                 [clojurewerkz/elastisch "2.2.1"]

                 ;; redis
                 [com.curiosity/crache "1.0.0-rc3" :exclusions [com.taoensso/carmine]]
                 [com.taoensso/carmine "2.13.1"]

                 ;; postgresql
                 [org.clojure/java.jdbc "0.3.6"]
                 ; postgresql driver
                 [org.postgresql/postgresql "9.4.1209"]
                 ; connection pooling
                 [com.zaxxer/HikariCP-java6 "2.3.13"]
                 [honeysql "0.7.0"]

                 ;; nrepl
                 [org.clojure/tools.nrepl "0.2.12"]
                 [cider/cider-nrepl "0.13.0-SNAPSHOT"]

                 ;; http
                 [javax.servlet/servlet-api "2.5"]
                 [ring "1.5.0"]
                 [ring/ring-codec "1.0.1"]
                 [ring/ring-headers "0.2.0"]
                 [ring/ring-defaults "0.2.1"]
                 [ring-middleware-format "0.7.0"]
                 [jumblerg/ring.middleware.cors "1.0.1"]
                 [ring-basic-authentication "1.0.5"]
                 [ring-undertow-adapter "0.2.2"]
                 [org.immutant/web  "2.1.4"]
                 [io.undertow/undertow-core "1.2.0.Beta4"]

                 ;; http error reporting
                 [raven-clj "1.4.2"]

                 ])
