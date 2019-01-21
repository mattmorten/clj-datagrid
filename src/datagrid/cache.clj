(ns datagrid.cache
  (:require [datagrid.cluster :as cluster]
            [datagrid.cache.server :as server]
            [clojure.core.async :as async :refer [<! <!! >! >!! close! go timeout go-loop]]
            [datagrid.dht :as dht]
            [datagrid.tcp :as tcp]))


(defn start-server [port node-count]
  (server/start-server port node-count))

(defn start-client [node-count]
  (let [[_ done-chan] (cluster/start-client node-count)
        chans (<!! done-chan)]
    {:servers chans
     :table (dht/ranges node-count)}))

(defn- server-for-key [{servers :servers table :table} key]
  (let [server-index (dht/lookup-key table key)]
    (println "Server-for-key (" (count servers)")" server-index "|" servers)
    (nth servers server-index)))


(defn pput [client key value]
  (let [server (server-for-key client key)]
    (<!! (tcp/send-request server [:put key value]))))

(defn gget [client key]
  (let [server (server-for-key client key)]
    (<!! (tcp/send-request server [:get key]))))
