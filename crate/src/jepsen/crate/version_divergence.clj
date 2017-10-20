(ns jepsen.crate.version-divergence
  "Writes a series of unique integer values to a table whilst causing network
   partitions and healing the network every 2 minutes.
   We will verify that each _version of a given row identifies a single value."
  (:refer-clojure :exclude [test])
  (:require [jepsen [core         :as jepsen]
                    [checker      :as checker]
                    [cli          :as cli]
                    [client       :as client]
                    [generator    :as gen]
                    [independent  :as independent]
                    [nemesis      :as nemesis]
                    [net          :as net]
                    [tests        :as tests]
                    [util         :as util :refer [timeout]]
                    [os           :as os]]
            [jepsen.os.debian     :as debian]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util  :as cu]
            [jepsen.control.net   :as cnet]
            [jepsen.crate.core    :as c]
            [clojure.string       :as str]
            [clojure.java.jdbc    :as j]
            [knossos.op           :as op])
  (:import (io.crate.shade.org.postgresql.util PSQLException)))

(defn client
  ([] (client nil))
  ([dbspec]
   (let [initialized? (promise)]
     (reify client/Client
       (setup! [this test node]
         (let [dbspec (c/await-client (c/get-node-db-spec node) node test)]
           (when (deliver initialized? true)
             (j/execute! dbspec
                         ["create table if not exists registers (
                          id     integer primary key,
                          value  integer)"])
             (j/execute! dbspec
                         ["alter table registers
                          set (number_of_replicas = \"0-all\")"]))
           (client dbspec)))

       (invoke! [this test op]
         (let [[k v] (:value op)]
           (timeout 500 (assoc op :type :fail, :error :timeout)
                    (try
                      (case (:f op)
                        :read (->> (j/query dbspec ["select value, \"_version\"
                                                    from registers where id = ?" k])
                                   first
                                   (independent/tuple k)
                                   (assoc op :type :ok, :value))

                        :write (let [res (j/execute! dbspec
                                                     ["insert into registers (id, value)
                                                      values (?, ?)
                                                      on duplicate key update
                                                      value = VALUES(value)" k v])]
                                 (assoc op :type :ok)))

                      (catch PSQLException e
                        (cond
                          (and (= 0 (.errorCode e))
                               (re-find #"blocked by: \[.+no master\];" (str e)))
                          (assoc op :type :fail, :error :no-master)

                          (and (= 0 (.errorCode e))
                               (re-find #"rejected execution" (str e)))
                          (do ; Back off a bit
                              (Thread/sleep 1000)
                              (assoc op :type :info, :error :rejected-execution))

                          :else
                          (throw e)))))))

       (teardown! [this test]
         )))))

(defn multiversion-checker
  "Ensures that every _version for a read has the *same* value."
  []
  (reify checker/Checker
    (check [_ test model history opts]
      (let [reads  (->> history
                        (filter op/ok?)
                        (filter #(= :read (:f %)))
                        (map :value)
                        (group-by :_version))
            multis (remove (fn [[k vs]]
                             (= 1 (count (set (map :value vs)))))
                           reads)]
        {:valid? (empty? multis)
         :multis multis}))))

(defn r [] {:type :invoke, :f :read, :value nil})
(defn w []
  (->> (iterate inc 0)
       (map (fn [x] {:type :invoke, :f :write, :value x}))
       gen/seq))

(defn test
  [opts]
  (merge tests/noop-test
         {:name    "version-divergence"
          :os      debian/os
          :db      (c/db (:tarball opts))
          :client  (client)
          :checker (checker/compose
                     {:multi    (independent/checker (multiversion-checker))
                      :timeline (timeline/html)
                      :perf     (checker/perf)})
          :concurrency 100
          :nemesis (nemesis/partition-random-halves)
          :generator (->> (independent/concurrent-generator
                            10
                            (range)
                            (fn [id]
                              (->> (gen/reserve 5 (r) (w)))))
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 120)
                                             {:type :info, :f :start}
                                             (gen/sleep 120)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit 360))}
         opts))
