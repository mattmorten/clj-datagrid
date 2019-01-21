(ns datagrid.core
  (:require [datagrid.cluster :as cluster]
            [clojure.core.async :as async :refer [<! <!! >! >!! close! go timeout go-loop]]
            [datagrid.cache :as cache]
            [datagrid.tcp :as tcp])
  (:gen-class))


(defn run-tcp-test []
  (let [server (tcp/new-server 10031 (fn [msg] "woah"))
        client (tcp/new-client 10031)]
    (println (<!! (tcp/send-request client {"hello" 1})))
    (println (<!! (tcp/send-request client {"hello" 2})))
    (<!! (timeout 5000))))


(defn start-server [port node-count]
  (async/thread
    (let [[_ done-chan] (cache/start-server port node-count)]
      (<!! done-chan)
      (println port "server initialied")))
  (<!! (timeout 500)))


(def keys
  ["it" "was" "the" "best" "of" "times" "a" "b" "c" "d" "e" "f" "g" "h"])

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (cluster/start-zookeeper-conn "127.0.0.1:2181")

  (start-server 10032 4)
  (start-server 10033 4)
  (start-server 10034 4)
  (start-server 10035 4)

  (let [client (cache/start-client 4)]
    (loop [i 1 [word & words] keys]
      (when (some? word)
        (cache/pput client word i)
        (recur (inc i) words)))

    (let [results (for [word keys]
                    (cache/gget client word))]

      (<!! (timeout 5000))
      (doseq [result results]
        (println result)))))




