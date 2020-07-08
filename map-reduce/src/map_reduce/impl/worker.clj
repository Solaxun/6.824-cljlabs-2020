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
        response (rpc/call! client rpc-name args)]
    (rpc/close! client)
    response))

(defn hashed-filename [worker-id entry]
  (-> entry :key ihash (mod 10) ((partial str "mr-" worker-id "-"))))

(defn write-kvs!
  [output {:keys [worker-id status last-checkin job n-reduce] :as worker}]
  (let [fname->entries (->> output (group-by (partial hashed-filename worker-id)))]
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
  (->> (:job worker)
       (mapcat file->kvs)
       (group-by :key)
       (map (fn [[k v]] [k (map :value v)]))
       (write-vals! worker reducef)))

(defn worker
  "map-reduce.worker calls this function."
  [mapf reducef]
  (let [worker (call! :Master/RegisterWorker nil)]
    (loop [{:keys [job stage] :as worker} (call! :Master/GetMapTask worker)]
      (println worker)
      (cond (= job :mapdone)
            ;; When no more map tasks, let master know where the worker saved it's files
            ;; move this workers intermdiate files out of waiting and into reduce queue
            (do (call! :Master/CompleteMapTask
                       (assoc worker :intermediate-files @intermediate-files))
                ;; GetReduceTask blocks until master knows all map tasks done
                (recur (call! :Master/GetReduceTask worker)))

            (= job :reducedone)
            (do (call! :Master/CompleteReduceTask worker)
                :finished)

            (= stage "map")
            (let [data (slurp job)]
              (write-kvs! (mapf job data) worker)
              (recur (call! :Master/GetMapTask worker)))

            (= stage "reduce")
            (do (reduce-stage! reducef worker)
                (recur (call! :Master/GetReduceTask worker)))))))
