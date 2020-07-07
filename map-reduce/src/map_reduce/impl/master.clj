(ns map-reduce.impl.master
  "Write your code here."
  (:require
   [go.net :as net]
   [go.rpc :as rpc]
   [clojure.set :as set]))

(def port 3000)
(def output-directory "mr-tmp")

(defprotocol IMaster
  (RegisterWorker [this _])
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

(defn register-worker [s n-reduce]
  (let [id (-> s :n-map-workers inc)]
    (-> s
        (assoc :n-map-workers id)
        (assoc :n-reduce-workers id)
        (update :workers assoc id
                {:worker-id id
                 :status "idle"
                 :last-checkin (.toString (java.time.LocalDateTime/now))
                 :job nil
                 :n-reduce n-reduce}))))

(defn assign-task [state worker stage]
  (-> state
      (update-in [:workers (:worker-id worker)]
                 assoc
                 :status "in-progress"
                 :stage stage
                 :last-checkin (.toString (java.time.LocalDateTime/now)))))

(defn complete-map-task [state {:keys [worker-id intermediate-files]}]
  (-> state
      (update-in [:workers worker-id]
                 merge {:status "completed"
                        :last-checkin (.toString (java.time.LocalDateTime/now))})
      (update :n-map-workers dec)
      (update :intermediate-files set/union intermediate-files)))

(defn complete-reduce-task [state worker]
  (-> state
      (update :n-reduce-workers dec)
      (update-in [:workers (:worker-id worker)] assoc
                 :last-checkin (.toString (java.time.LocalDateTime/now)))))

(defn ifile->reduceid [fpath]
  (last (clojure.string/split fpath #"-")))

(defn fill-reduce-q [reduceq state]
  (println "map jobs done, starting reduce tasks...")
  (let [{:keys [intermediate-files n-reduce-workers]} state
        shutdown-msgs (repeat n-reduce-workers :reducedone)]
    (doseq [[rid files] (group-by ifile->reduceid intermediate-files)]
      (.add reduceq files))
    (doseq [s shutdown-msgs]
      (.add reduceq s))))

(defn map-jobs-finished? [state]
  (zero? (:n-map-workers state)))

(defrecord Master [map-tasks reduce-tasks state n-reduce]
  IMaster
  (RegisterWorker
    [this _]
    (.add map-tasks :mapdone)
    (let [{:keys [n-map-workers workers] :as s} (swap! state register-worker n-reduce)]
      (get-in s [:workers n-map-workers])))
  (GetMapTask
    [this worker]
    (let [s (swap! state assign-task worker "map")
          w (get-in s [:workers (:worker-id worker)])
          job (.take map-tasks)]
      (assoc w :job job)))
  (GetReduceTask
    [this worker]
    (println "[" (:worker-id worker) "]"  ": waiting for reduce task ...")
    (let [s (swap! state assign-task worker "reduce")
          w (get-in s [:workers (:worker-id worker)])
          job (.take reduce-tasks)]
      (assoc w :job job)))
  (CompleteMapTask
    [this worker]
    (let [s (swap! state complete-map-task worker)]
      (when (map-jobs-finished? s)
        (fill-reduce-q reduce-tasks s))))
  (CompleteReduceTask
    [this worker]
    (println "[" (:worker-id worker) "]"  ": reduce job done.")
    (swap! state complete-reduce-task worker)))

(defn master
  [tasks n-reduce]
  (->Master (java.util.concurrent.LinkedBlockingQueue. tasks)
            (java.util.concurrent.LinkedBlockingQueue.)
            (atom {:n-map-workers 0
                   :n-reduce-workers 0
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
        {:keys [n-map-workers n-reduce-workers]} @(:state master)]
    (println  n-reduce-workers " - reduce workers")
    (and (empty? map-tasks)
         (empty? reduce-tasks)
         (zero? n-reduce-workers))))

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
