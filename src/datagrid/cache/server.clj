(ns datagrid.cache.server
  (:require [datagrid.cluster :as cluster]
            [clojure.core.async :as async :refer [<! <!! >! >!! close! go timeout go-loop]]
            [datagrid.messaging :as messaging]
            [datagrid.tcp :as tcp]))

(def ok-response [:ok])
(defn pong [node-name _]
  [:pong (str "hello from " node-name)])

(defn cache-put [node-name cache [_ key value]]
  (swap! cache assoc key value)
  (println "Cache ["node-name"]: PUT |" @cache)
  ok-response)

(defn cache-get [node-name cache [_ key]]
  (println "Cache ["node-name"]: GET | "key" |" @cache)
  [:got node-name key (get @cache key)])

(defn create-cache []
  (atom {}))

(defn mk-handler-fn [cache]
  (fn [node-name event]
    (println "Cache ["node-name"]: " event)
    (let [type (keyword (first event))]
      (case type
        :ping (pong node-name event)
        :put (cache-put node-name cache event)
        :get (cache-get node-name cache event)))))

(defn start-server [port node-count]
  (let [cache (create-cache)
        handler-fn (mk-handler-fn cache)]
    (cluster/start-server port node-count handler-fn)))


