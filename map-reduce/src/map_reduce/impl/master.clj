(ns map-reduce.impl.master
  "Write your code here."
  (:require
   [go.net :as net]
   [go.rpc :as rpc]
   [clojure.set :as set]))

(def port 3000)
(def output-directory "mr-tmp")

(defprotocol IMaster
  (GetMapTask [this worker])
  (GetReduceTask [this worker])
  (CompleteMapTask [this worker])
  (CompleteReduceTask [this worker]))

(def tasks
  (->>  "./resources/books"
        (clojure.java.io/file)
        file-seq
        (filter #(.isFile %))
        (map #(.getPath %))))

(defn make-worker [worker id n-reduce]
  {:worker-id id
   :stage (:stage worker "map")
   :last-checkin (.toString (java.time.LocalDateTime/now))
   :job (:job worker nil)
   :n-reduce n-reduce})
;; if worker, only change sstage and time, return as latest worker
(defn get-map-task [s worker n-reduce]
  (let [id (get worker :worker-id (-> s :active-map-workers inc))
        w (make-worker worker id n-reduce)]
    (-> s
        (update :workers assoc id w)
        (assoc :current-worker w)
        (update :active-map-workers (if (some? worker) identity inc))
        (update :active-reduce-workers (if (some? worker) identity inc)))))

(defn complete-map-task [state {:keys [worker-id intermediate-files]}]
  (-> state
      (update-in [:workers worker-id] assoc
                 :last-checkin (.toString (java.time.LocalDateTime/now)))
      (update :active-map-workers dec)
      (update :intermediate-files set/union intermediate-files)))

(defn reduce-task [state action {:keys [worker-id] :as worker}]
  (let [rworker-update {:get-task identity :complete-task dec}
        w (assoc worker
                 :last-checkin (.toString (java.time.LocalDateTime/now))
                 :stage "reduce")]
    (-> state
        (update :active-reduce-workers (get rworker-update action))
        (assoc :current-worker w)
        (assoc-in [:workers worker-id] w))))

(defn ifile->reduceid [fpath]
  (last (clojure.string/split fpath #"-")))

(defn fill-reduce-q [reduceq state]
  #_(println "all map jobs done, starting reduce tasks...")
  (let [{:keys [intermediate-files active-reduce-workers]} state
        shutdown-msgs (repeat active-reduce-workers :reducedone)]
    (doseq [[rid files] (group-by ifile->reduceid intermediate-files)]
      (.add reduceq files))
    (doseq [s shutdown-msgs]
      (.add reduceq s))))

(defn map-jobs-finished? [state]
  (zero? (:active-map-workers state)))

(defrecord Master [map-tasks reduce-tasks state n-reduce]
  IMaster
  (GetMapTask
    [this worker]
    #_(println "["(:worker-id worker) "]" " - n mappers: " (:active-map-workers @state))
    (when-not worker (.add map-tasks :mapdone))
    (let [s (swap! state get-map-task worker n-reduce)
          w (:current-worker s)]
      (assoc w :job (.take map-tasks))))
  (GetReduceTask
    [this worker]
    #_(println "["(:worker-id worker) "]" " - n reducers: " (:active-reduce-workers @state))
    (let [s (swap! state reduce-task :get-task worker)
          w (:current-worker s)]
      (assoc w :job (.take reduce-tasks))))
  (CompleteMapTask
    [this worker]
    (let [s (swap! state complete-map-task worker)]
      (when (map-jobs-finished? s)
        (fill-reduce-q reduce-tasks s))))
  (CompleteReduceTask
    [this worker]
    #_(println "["(:worker-id worker) "]" " - reducer q: " (:reduce-tasks this))
    (swap! state reduce-task :complete-task worker)))

(defn master
  [tasks n-reduce]
  (->Master (java.util.concurrent.LinkedBlockingQueue. tasks)
            (java.util.concurrent.LinkedBlockingQueue.)
            (atom {:active-map-workers 0
                   :active-reduce-workers 0
                   :intermediate-files #{}
                   :workers {}})
            n-reduce))

(defn start-server
  "Start a thread that listens for RPCs from worker.clj"
  [master]
  (let [server (rpc/new-server)
        _      (rpc/register server master)
        _      (rpc/handle-http server)
        l      (net/listen port)]
    (rpc/serve server l)
    server))

(defn all-jobs-complete? [master]
  (let [{:keys [map-tasks reduce-tasks]} master
        {:keys [active-map-workers active-reduce-workers]} @(:state master)]
    #_(println  active-reduce-workers " - reduce workers")
    (and (empty? map-tasks)
         (empty? reduce-tasks)
         (zero? active-reduce-workers))))

(defn done
  "map-reduce.master calls `done` periodically to find out
  if the entire job has finished."
  [master]
  (all-jobs-complete? master))

(defn make-master
  "Create a Master.

  map-reduce.master calls this function.
  `n-reduce` is the number of reduce tasks to use."
  [files n-reduce]
  (let [m (master tasks n-reduce)
        _ (start-server m)]
    m))

(defn -main []
  (let [m (make-master tasks 10)]
    (while (not (done m))
      (Thread/sleep 1000))))
