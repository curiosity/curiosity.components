(defproject curiosity.components "0.2.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-optons ["-Xmx1G"]
  :repl-options {:timeout 120000}
  :repositories [["s3" {:url "s3p://curiosity-java-jars-private/jars/"
                        :username :env/aws_access_key
                        :passphrase :env/aws_secret_key}]]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [prismatic/plumbing "0.4.4"]
                 [prismatic/schema "0.4.3"]
                 [com.taoensso/encore "2.4.2"]
                 [curiosity.utils "0.5.0" :exclusions [com.taoensso/encore]]
                 [environ "1.0.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.emidln/destructured-component "0.3.0"]
                 [slingshot "0.12.2"]
                 [org.clojure/tools.logging "0.3.1"]

                 ;; serialization
                 ; DateTime
                 [joda-time/joda-time "2.5"]
                 ; JSON
                 [cheshire "5.4.0"]

                 ;; metrics component
                 [metrics-clojure "2.4.0"]
                 [metrics-clojure-jvm "2.4.0"]
                 [metrics-clojure-ring "2.4.0"]

                 ;; s3 and sqs
                 [com.amazonaws/aws-java-sdk "1.9.36"]

                 ; communicate with s3
                 [com.curiosity/clj-aws-s3 "0.4.0" :exclusions [joda-time com.fasterxml.jackson.core/jackson-annotations]]
                 ; communicate with sqs
                 [com.cemerick/bandalore "0.0.6" :exclusions [joda-time/joda-time]]

                 ;; elasticsearch
                 [clojurewerkz/elastisch "2.2.0-beta4"]

                 ;; redis
                 [com.curiosity/crache "1.0.0-rc3" :exclusions [com.taoensso/carmine]]
                 [com.taoensso/carmine "2.11.1"]

                 ;; postgresql
                 [org.clojure/java.jdbc "0.3.6"]
                 ; postgresql driver
                 [org.postgresql/postgresql "9.4-1201-jdbc41"]
                 ; connection pooling
                 [com.mchange/c3p0 "0.9.5"]

                 ;; nrepl
                 [org.clojure/tools.nrepl "0.2.10"]

                 ;; http
                 [javax.servlet/servlet-api "2.5"]
                 [ring "1.4.0"]
                 [ring/ring-codec "1.0.0"]
                 [ring/ring-headers "0.1.3"]
                 [ring/ring-json "0.3.1"]
                 [ring/ring-defaults "0.1.5"]
                 [jumblerg/ring.middleware.cors "1.0.1"]
                 [ring-basic-authentication "1.0.5"]
                 [ring-undertow-adapter "0.2.2"]
                 [io.undertow/undertow-core "1.2.0.Beta4"]

                 ;; http error reporting
                 [raven-clj "1.3.1"]

                 ])
