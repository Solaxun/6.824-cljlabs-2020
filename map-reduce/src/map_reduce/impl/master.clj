(ns map-reduce.impl.master
  "Write your code here."
  (:require
   [go.net :as net]
   [go.rpc :as rpc]))

(def port 3000)
(def output-directory "mr-tmp")

(defprotocol IMaster
  (RegisterWorker [this _])
  (GetMapTask [this _])
  (CompleteTask [this status]))

(defn task-from-fpath [fpath]
  {:type "map"
   :status "idle"
   :time-started nil
   :time-completed nil
   :task-fpath fpath})

(def tasks
  (->>  "./resources/books"
        (clojure.java.io/file)
        file-seq
        (filter #(.isFile %))
        (map #(.getPath %))))

(defn register-worker [s n-reduce]
  (let [id (-> s :total-workers inc)]
    (-> s
        (assoc :total-workers id)
        (assoc id  {:worker-id id
                    :status "idle"
                    :last-checkin (.toString (java.time.LocalDateTime/now))
                    :job nil
                    :n-reduce n-reduce}))))

(defn assign-task [s worker-id]
  (-> s
      (update worker-id
              assoc
              :status "in-progress"
              :last-checkin (.toString (java.time.LocalDateTime/now)))))

(defrecord Master [map-tasks worker-state n-reduce]
  IMaster
  (RegisterWorker
    [this _]
    ;; shutdown once all map tasks taken off tasks queue
    (.add map-tasks :shutdown)
    (let [{:keys [total-workers] :as wstate}
          (swap! worker-state register-worker n-reduce)]
      (get wstate total-workers)))
  (GetMapTask
    [this worker-id]
    (let [wstate (swap! worker-state assign-task worker-id)
          w (get wstate worker-id)
          job (.take map-tasks)]
      #_(println job)
      #_(println "...." w)
      (assoc w :job job)))
  (CompleteTask
    [this status]
    (let [{:keys [worker-id task-type]} status]
      #_(println ["[completed]: " worker-id task-type])
      (swap! worker-state update worker-id merge
             {:status "completed"
              :task-type task-type
              :last-checkin (.toString (java.time.LocalDateTime/now))}))))

(defn master
  [tasks n-reduce]
  (->Master (java.util.concurrent.LinkedBlockingQueue. tasks)
            (atom {:total-workers 0})
            n-reduce))
;; (def m (master tasks 10))
;; (RegisterWorker m nil)
;; (GetMapTask m 3)
;; (CompleteTask m {:worker-id 3 :task-type "reduce"})

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
  (let [worker-state @(:worker-state master)]
    (when (pos? (:total-workers worker-state))
      (every? (fn [[worker-id {:keys [status task-type] :as worker}]]
                #_(println worker-id status task-type)
                (and (= status "completed")
                     (= task-type "reduce")))
              (dissoc worker-state :total-workers)))))

(defn done
  "map-reduce.master calls `done` periodically to find out
  if the entire job has finished."
  [master]
  ;; only tracking map completion, need to do same for reduce
  ;; and only once all reduce workers finished are we done
  (all-jobs-complete? master)
  #_false)

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
