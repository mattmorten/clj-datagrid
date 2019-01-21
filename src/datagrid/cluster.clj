(ns datagrid.cluster
  (:require [zookeeper :as zk]
            [datagrid.tcp :as tcp]
            [clojure.core.async :as async :refer [<! <!! >! >!! close! go timeout go-loop chan]]))


(def parent "/cls")

(def zookeeper-connection (atom 0))

(defn other-children [me children]
  (let [children (->> children
                   (map #(str parent "/" %))
                   (into #{}))]
    (if (some? me)
      (disj children me)
      children)))

(defn data-watcher-fn [me target-nodename other-zmq-atom other-count complete-chan event]
  (let [data (String. ^bytes (:data event) "UTF-8")]
    (println "data-watcher-fn ("me"): " data)
    (swap! other-zmq-atom assoc target-nodename data)
    (when (= other-count (count @other-zmq-atom))
      (println "Got all zmq addresses ("me"):" @other-zmq-atom)
      (let [clients (for [[nodename port-str] @other-zmq-atom]
                      (do
                        (println "data-watcher-fn ("me"): attempting to connect to " port-str)
                        (let [client (tcp/new-client (Long/parseLong port-str))]
                          (println "tcp-client["me"] got: " (<!! (tcp/send-request client [:ping])))
                          ;; return the client
                          [nodename client])))
            clients (->> clients
                         (sort-by first)
                         (map second)
                         (into []))]
        (>!! complete-chan clients)
        (close! complete-chan)))))


(defn start-zookeeper-conn [zookeeper-addr]
  (reset! zookeeper-connection (zk/connect zookeeper-addr)))

(defn search-servers
  "For servers, provide your own server nodenam"
  ([node-count nodename]
   ;; Create an atom which will store the zmq addresses
   (let [other-zmq-atom (atom {})
         complete-chan (chan 1)]

     (go-loop []
       (let [children (zk/children @zookeeper-connection parent)]
         (if (= node-count (count children))
           (let [others (other-children node-count children)]
             (doseq [other-child others]
               (zk/data @zookeeper-connection other-child :callback (partial data-watcher-fn nodename other-child other-zmq-atom (count others) complete-chan))))
           (do
             (<! (timeout 500))
             (recur)))))

     [other-zmq-atom complete-chan]))


  ([node-count]
   (search-servers node-count nil)))

(defn start-server [tcp-port node-count handler-fn]

  (let [nodename (zk/create-all @zookeeper-connection (str parent "/server-") :sequential? true :persistent? false)
        version (:version (zk/exists @zookeeper-connection nodename))]

    (println "I am " nodename)

    (tcp/new-server tcp-port (partial handler-fn nodename))

    (println "Here " tcp-port)
    (zk/set-data @zookeeper-connection nodename (.getBytes (str tcp-port) "UTF-8") version)

    (println "Again " tcp-port)
    (search-servers node-count nodename)))


(defn start-client [node-count]

  (search-servers node-count))








