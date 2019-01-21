(ns datagrid.dht
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.spec.test.alpha :as stest]))

(def hash-ceil 10000)

(s/def ::hash (and integer? #(<= 0 % 9999)))
(s/def ::range (s/tuple ::hash ::hash))

(s/def ::segments (s/and (s/coll-of ::range)
                         #(when (not (empty? %))
                            (let [[start _] (first %)
                                  [_ end] (last %)]
                              (and (= 0 start)
                                   (= 9999 end))))))

(defn hash-key
  [string]
  (mod (hash string) hash-ceil))

(s/fdef hash-key
        :args (s/cat ::in string?)
        :ret ::hash)

(defn ranges
  [n]
  (if (= n 0)
    []
    (let [delta (/ hash-ceil n)]
      (loop [i 0 start 0.0 result []]
        (let [end (-> start (+ delta) int (- 1))
              segment [(int start) end]
              result (conj result segment)]
          (if (= i (- n 1))
            result
            (recur (inc i) (+ start delta) result)))))))

(s/fdef ranges
        :args (s/cat ::n pos-int?)
        :ret ::segments)

(defn ranges-for-keys [ranges & keys]
  (map #(get ranges %) keys))

(defn in-range? [[start end] hash]
  (<= start hash end))

(defn lookup-key-from-hash [ranges hash]
  ;; must improve
  (->> ranges
    (map-indexed vector)
    (filter (fn [[_ item]] (in-range? item hash)))
    first first))

(defn lookup-key [ranges item]
  (lookup-key-from-hash ranges (hash-key item)))

(s/fdef lookup-key
        :args (s/cat ::segments ::segments ::in string?)
        :ret pos-int?)






