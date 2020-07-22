(ns map-reduce.impl.worker
  "Write your code here."
  (:require [go.fnv :as fnv]
            [go.rpc :as rpc]
            [clojure.java.io :as io]
            [map-reduce.plugin.wc :as wc]
            [clojure.string :as string]))

(def output-directory "mr-tmp")
(def port 3000)
(def intermediate-files (atom #{}))

(defn ihash
  "Use ihash(key) % `n-reduce` to choose the reduce
  task number for each key/value pair emitted by map."
  [s]
  (bit-and (fnv/fnv1a32 s) 0x7fffffff))

(defn call!
  [rpc-name args]
  (let [client   (rpc/dial port)
        response (try
                   (rpc/call! client rpc-name args)
                   (catch Exception e))]
    (rpc/close! client)
    response))

(defn hashed-filename [worker-id entry]
  (-> entry :key ihash (mod 10) ((partial str "mr-" worker-id "-"))))

(defn map-stage!
  [mapf {:keys [worker-id status last-checkin job n-reduce] :as worker}]
  (let [fname->entries (->> (:fpath job)
                            slurp
                            ((partial mapf (:fpath job)))
                            (group-by (partial hashed-filename worker-id)))]
    (doseq [[fname kvpairs] fname->entries]
      (with-open [writer (io/writer (io/file output-directory fname) :append true)]
        (doseq [{k :key v :value} kvpairs]
          (.write writer (format "%s %s\n" k v))))
      (swap! intermediate-files conj (str output-directory "/" fname)))))

(defn write-vals! [worker reducef kv-counts]
  (with-open [writer (io/writer (io/file output-directory
                                         (str "mr-out-" (:worker-id worker)))
                                :append true)]
    (doseq [[k cnt] kv-counts]
      (.write writer (format "%s %s\n" k (reducef k cnt))))))

(defn file->kvs [f]
  (->> f
       slurp
       string/split-lines
       (map (fn [kv]
              (let [[k v] (string/split kv #" ")]
                {:key k :value (read-string v)})))))

(defn reduce-stage! [reducef worker]
  (->> (get-in worker [:job :fpath])
       (mapcat file->kvs)
       (group-by :key)
       (map (fn [[k v]] [k (map :value v)]))
       (write-vals! worker reducef)))

(defn worker
  "map-reduce.worker calls this function."
  [mapf reducef]
  (loop [{{stage :stage} :job :as worker} (call! :Master/GetTask nil)]
    (println worker)
    (when worker ; assume no response means work done (master could be down though)
      (if (= stage "map")
        (do (map-stage! mapf worker)
            (call! :Master/CompleteMapTask
                   (assoc worker :intermediate-files @intermediate-files)))
        (do (reduce-stage! reducef worker)
            (call! :Master/CompleteReduceTask
                   worker)))
      (recur (call! :Master/GetTask worker)))))
