(ns curiosity.components.rate-limit
  (:require [clojure.core.async :as async :refer
             [<! >! <!! >!! chan timeout go go-loop alts! close! onto-chan thread]]
            [clojure.core.async.impl.protocols :refer [ReadPort WritePort]]
            [schema.core :as s]
            [plumbing.core :refer [defnk]]
            [taoensso.timbre :as log])
  (:import [java.util.concurrent Executors TimeUnit]))

(log/handle-uncaught-jvm-exceptions!)

(s/defn clock [ms :- s/Int]
  "Sends a clock every ms milliseconds (assuming it is being consumed)"
  (let [c (chan 1)
        f #(>!! c (System/currentTimeMillis))]
    (let [x (Executors/newSingleThreadScheduledExecutor)]
      (.scheduleWithFixedDelay x f 0 ms TimeUnit/MILLISECONDS))
    c))

(s/defn other-clock [ms :- s/Int]
  (let [c (chan 1)]
    (go-loop []
      (>! c (do (<! (timeout ms)) (System/currentTimeMillis)))
      (recur))
    c))

(defnk rate-limit
  [in         :- (s/protocol ReadPort)
   clock      :- (s/protocol ReadPort)
   {limit     :- s/Int 1000}]
  (let [out (chan)]
    (go-loop [buf []]
      (if (>= (count buf) limit)
        (do (<! clock)
            (>! out buf)
            (recur []))
        (let [[v ch] (alts! [in clock])]
          (if (= ch clock)
            (do (>! out buf)
                (recur []))
            (recur (conj buf v))))))
    out))

(s/defn range-chan
  ([]
   (range-chan Long/MAX_VALUE))
  ([n :- s/Int]
   (let [c (chan 1)]
     (go-loop [i 0]
       (if (< i n)
         (do (>! c i)
             (recur (inc i)))
         (close! c)))
     c)))

(defnk partitioned-rate-limit
  [in :- (s/protocol ReadPort)
   clock :- (s/protocol ReadPort)
   limit :- s/Int
   partitions :- s/Int]
  (let [limited (rate-limit {:in in
                             :clock clock
                             :limit (* limit partitions)})
        xf (comp (partition-all limit)
                 (map (partial filter some?))
                 (filter seq))
        out (chan 100)]
    (go-loop []
      (let [vs (<! limited)]
        (if (every? nil? vs)
          (do (close! out)
              ::done)
          (do
            (doseq [x (eduction xf vs)]
              (>! out x))
            (recur)))))
    out))


(comment

  (let [limited (rate-limit {:in (range-chan 100)
                             :clock (clock 1000)
                             :limit 10})]
    (go-loop []
      (let [v (<! limited)]
        (if (every? nil? v)
          ::done
          (do (prn v)
              (recur))))))

  (let [c (partitioned-rate-limit {:in (range-chan 2000)
                                   :clock (clock 1000)
                                   :limit 50
                                   :partitions 3})]
    (->> (<!! (go-loop [times []]
                (let [v (<! c)]
                  (if (and v (not= v ::done))
                    (do
                      (prn v)
                      (recur (conj times (System/currentTimeMillis))))
                    times))))
         (partition-all 3)
         (map (fn [[uno dos tres]]
                (when (and tres uno)
                  (- tres uno))))))

  (let [limited (rate-limit {:in (range-chan 100)
                             :clock (clock 2000)
                             :limit 30})]
    (go-loop []
      (let [v (<! limited)]
        (if (every? nil? v)
          ::done
          (do (->> (partition-all 10 v)
                   (mapv (partial filter some?))
                   (filter seq)
                   prn)
              (recur))))))


  )

