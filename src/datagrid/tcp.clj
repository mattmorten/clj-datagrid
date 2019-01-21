(ns datagrid.tcp
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async :refer [<! <!! >! >!! close! go timeout go-loop chan]]
            [jsonista.core :as j])
  (:import [java.net ServerSocket Socket]
           (java.io OutputStream InputStream InputStreamReader BufferedReader)))

(defn make-buffered-reader
  [^Socket socket]
  (let [^InputStream is (.getInputStream socket)
        ^InputStreamReader isr (InputStreamReader. is)]
    (BufferedReader. isr)))

(defn send-response
  "Send the given string message out over the given socket"
  [socket msg]
  (let [writer (io/writer socket)]
    (.write writer msg)
    (.flush writer)))


(defn new-server [port handler]
  (let [running (atom true)]
    (future
      (with-open [server-sock (ServerSocket. port)]
        (while @running
          (printf "TCP Server [%d] ready to receive conn\n" port)
          (let [^Socket sock (.accept server-sock)]
            (printf "TCP Server [%d] accepted conn\n" port)
            (async/thread
              (let [br (make-buffered-reader sock)]
                (while @running
                 (let [msg-in (.readLine br)
                       json (j/read-value msg-in)]
                   (if (some? json)
                     (do
                       (println "Server IN: " json)
                       (when-let [response (handler json)]
                         (println "Server OUT: " response)
                         (send-response sock (str (j/write-value-as-string response) "\n"))))
                     (do
                       (println "Socket closed")
                       (reset! running false)))))))))))
    running))

(defn new-client
  [^long port]
  (let [input-chan (chan 50)]
    (go
      (with-open [client-socket (Socket. "localhost" port)]
        (let [^OutputStream output-stream (.getOutputStream client-socket)]
          (loop []
            (let [[msg response-chan] (<! input-chan)]
              (let [^String s (str (j/write-value-as-string msg) "\n")]
                (println "client writing: " s)
                (.write output-stream (.getBytes s "UTF-8"))
                ;(.write output-stream ^bytes b)
                (.flush output-stream)
                (println "done")
                (when response-chan
                  (println "Waiting for response")
                  (let [br (make-buffered-reader client-socket)
                        msg-in (.readLine br)
                        json (j/read-value msg-in)]
                    (>! response-chan json)
                    (close! response-chan)))
                (recur)))))))

    input-chan))


(defn send-message [client message]
  (>!! client [message nil]))
(defn send-request [client message]
  (let [response-chan (chan 1)]
    (>!! client [message response-chan])
    response-chan))




