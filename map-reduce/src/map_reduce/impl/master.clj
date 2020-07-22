(ns map-reduce.impl.master
  "Write your code here."
  (:require
   [go.net :as net]
   [go.rpc :as rpc]
   [clojure.set :as set])
  (:import
   [java.util.concurrent LinkedBlockingQueue]
   [java.time LocalDateTime Duration]))

(def port 3000)
(def output-directory "mr-tmp")

(defprotocol IMaster
  (GetTask [this worker])
  (CompleteMapTask [this worker])
  (CompleteReduceTask [this worker]))

(def tasks
  (->>  "./resources/books"
        (clojure.java.io/file)
        file-seq
        (filter #(.isFile %))
        (map #(hash-map :stage "map" :fpath (.getPath %)))))

(defn make-worker [worker id n-reduce]
  {:worker-id id
   :last-update (.toString (java.time.LocalDateTime/now))
   :job (:job worker)
   :n-reduce n-reduce})

(defn get-task [s worker n-reduce]
  (let [id (:worker-id worker (-> s :worker-id inc))
        w (make-worker worker id n-reduce)]
    (-> s
        (assoc  :worker-id id)
        (update :workers assoc id w)
        (assoc  :newest-worker w))))

(defn complete-map-task [state {:keys [worker-id intermediate-files]}]
  ;; TODO: if this worker was marked as failed due to a delay and then eventually
  ;; responded, need to NOT update intermediate files and NOT decrement work remaining
  (when-not (contains? (:failed-workers state) worker-id)
    (-> state
        (update-in [:workers worker-id] assoc
                   :last-update (.toString (java.time.LocalDateTime/now)))
        (update :map-jobs-remaining dec)
        (update :intermediate-files set/union intermediate-files))))

(defn complete-reduce-task [state {:keys [worker-id] :as worker}]
  (when-not (contains? (:failed-workers state) worker-id)
    (-> state
        (update-in [:workers worker-id] assoc
                   :last-update (.toString (java.time.LocalDateTime/now)))
        (update :reduce-jobs-remaining dec))))

(defn ifile->reduceid [fpath]
  (last (clojure.string/split fpath #"-")))

(defn start-reducing [tasks state-atom state-val]
  #_(println "all map jobs done, starting reduce tasks...")
  (let [{:keys [intermediate-files workers]} state-val
        reduce-jobs (group-by ifile->reduceid intermediate-files)]
    (swap! state-atom assoc :reduce-jobs-remaining (count reduce-jobs))
    (doseq [[rid files] reduce-jobs]
      (.add tasks {:stage "reduce" :fpath files}))
    ;; sometimes it seems pending takes might finish consuming all the
    ;; reduce jobs above, resulting in a zero cnt of rjobs remaining and
    ;; and empty queue before all of the "finished" flags below can be
    ;; inserted.  Then some worker will try to contact the closed master
    ;; and receive an error
    #_(dotimes [r (count workers)]
      (.add tasks {:stage "finished"}))))

(defn map-jobs-finished? [state]
  (zero? (:map-jobs-remaining state)))

(defrecord Master [tasks state n-reduce]
  IMaster
  (GetTask
    [this worker]
    #_(println "["(:worker-id worker) "]" " - n mappers: " (:active-map-workers @state))
    (let [s (swap! state get-task worker n-reduce)
          w (:newest-worker s)]
      (assoc w :job (.take tasks))))
  (CompleteMapTask
    [this worker]
    (let [s (swap! state complete-map-task worker)]
      (when (map-jobs-finished? s)
        (start-reducing tasks state s))))
  (CompleteReduceTask
    [this worker]
    #_(println "["(:worker-id worker) "]" " - reducer q: " (:reduce-tasks this))
    (swap! state complete-reduce-task worker)))

(defn datetime-str->datetime [s]
  (java.time.LocalDateTime/parse s))

(defn seconds-between [t0 t1]
  (/ (java.time.Duration/between t0 t1)
     1000))

(defn job-stuck? [[id worker]]
  (let [t0 (:last-update worker)]
    (> (seconds-between t0 (java.time.LocalDateTime/now))
       9.5)))

(defn update-failed-workers [state failed-workers]
  (-> state
      (update :failed-workers into failed-workers)
      #_(update :map-jobs-remaining ?)
      #_(update :reduce-jobs-remaining ?)))

(defn reassign-failed-jobs [master]
  (when-let [failed (filter job-stuck? (-> master :state deref :workers))]
    (println "failure: " failed)
    (swap! (:state master) update-failed-workers failed)
    (doseq [[id worker] failed]
      (.add (:tasks master)
            (:job worker)))))

(defn master
  [tasks n-reduce]
  (->Master (java.util.concurrent.LinkedBlockingQueue. tasks)
            (atom {:worker-id 0
                   :newest-worker nil
                   :failed-workers {}
                   :map-jobs-remaining (count tasks)
                   :reduce-jobs-remaining nil
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

(defn done
  "map-reduce.master calls `done` periodically to find out
  if the entire job has finished."
  [master]
  (when-let [rjobs (-> master :state deref :reduce-jobs-remaining)]
    #_(println rjobs (:tasks master))
    (and (zero? rjobs)
         (empty? (:tasks master)))))

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
      (reassign-failed-jobs m)
      (Thread/sleep 1000))))

;; if worker fails, we will have not completed task so map tasks remaining
;; stays at the right numer.  ID could either be recycled by reducing worker-id
;; in state map, or leaving it as "retired" and just assign new ID.  Then if worker
;; later responds (it was delayed, not dead) we need to find a way to ignore it's
;; intermediate work if that work was already done by another worker.  If the ID can
;; be reused, we need to somehow know it's work is outdated e.g. timestamps.  If the ID
;; is retired, we can just ignore all retired jobs
