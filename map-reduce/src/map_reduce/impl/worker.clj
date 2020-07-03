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

;;; don't think I need this Worker record or constructor
;;; worker fn already receives mapf and reduce f
#_(defrecord Worker [mapf reducef])

#_(defn new-worker
  [mapf reducef]
  (map->Worker {:mapf    mapf
                :reducef reducef}))

(defn write-kvs
  [output {:keys [worker-id status last-checkin job n-reduce] :as worker}]
  (doseq [{k :key v :value} output]
    ;; mr-n-n.txt
    (let [f (io/file output-directory
                          (str "mr-" worker-id "-" (mod (ihash k) n-reduce)))]
      (with-open [writer (io/writer f :append true)]
        (.write writer (format "%s %s\n" k v)))
      (swap! intermediate-files conj f)))
  (call! :Master/CompleteTask {:worker-id worker-id :task-type "map"}))

(defn write-vals [worker-id reducef kv-counts]
  (with-open [writer (io/writer (io/file output-directory (str "mr-out-" worker-id)))]
    (doseq [[k cnt] kv-counts]
      (.write writer (format "%s %s\n" k (reducef k cnt)))))
  (call! :Master/CompleteTask {:worker-id worker-id :task-type "reduce"}))

(defn file->kvs [f]
  (->> f
       slurp
       string/split-lines
       (map (fn [kv]
              (let [[k v] (string/split kv #" ")]
                {:key k :value (read-string v)})))))

(defn reduce-stage [reducef files worker]
  (->> files
       ;; only process those belonging to this worker since the atom
       ;; has results from every worker.  When running separate processes
       ;; e.g. from terminal, this shouldn't be an issue
       (filter #(-> % .getName
                    (string/split #"-")
                    second read-string
                    (= (:worker-id worker))))
       (mapcat file->kvs)
       (group-by :key)
       (map (fn [[k v]] [k (map :value v)]))
       (write-vals (:worker-id worker) reducef)))

(defn worker
  "map-reduce.worker calls this function."
  [mapf reducef]
  (let [{id :worker-id} (call! :Master/RegisterWorker nil)]
    (loop [{:keys [worker-id status last-checkin job] :as worker}
           (call! :Master/GetMapTask id)]
      (println worker)
      (if (= job :shutdown)
        (reduce-stage reducef @intermediate-files worker)
        (let [data (slurp job)]
          (write-kvs (mapf job data) worker)
          (recur (call! :Master/GetMapTask id)))))))

#_(dotimes [i 6] (future (worker wc/mapf wc/reducef)))
#_(worker wc/mapf wc/reducef)
#_(call! :Master/CompleteTask {:worker-id :mcw :task-type "reduce"})
