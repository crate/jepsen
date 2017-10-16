(ns jepsen.crate.core
  (:require [jepsen [core         :as jepsen]
                    [db           :as db]
                    [control      :as c :refer [|]]
                    [checker      :as checker]
                    [cli          :as cli]
                    [client       :as client]
                    [generator    :as gen]
                    [independent  :as independent]
                    [nemesis      :as nemesis]
                    [net          :as net]
                    [tests        :as tests]
                    [util         :as util :refer [meh
                                                   timeout
                                                   with-retry]]
                    [os           :as os]]
            [jepsen.os.debian     :as debian]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util  :as cu]
            [jepsen.control.net   :as cnet]
            [cheshire.core        :as json]
            [clojure.string       :as str]
            [clojure.java.jdbc    :as j]
            [clojure.java.io      :as io]
            [clojure.java.shell   :refer [sh]]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [knossos.op           :as op])
  (:import (java.net InetAddress)
           (io.crate.shade.org.postgresql.util PSQLException)
           (org.elasticsearch.rest RestStatus)
           (org.elasticsearch.common.unit TimeValue)
           (org.elasticsearch.common.settings
             Settings)
           (org.elasticsearch.common.transport
             InetSocketTransportAddress)
           (org.elasticsearch.client.transport
             TransportClient)
           (org.elasticsearch.transport.client
             PreBuiltTransportClient)))

(defn map->kw-map
  "Turns any map into a kw-keyed persistent map."
  [x]
  (condp instance? x
    java.util.Map
    (reduce (fn [m pair]
              (assoc m (keyword (key pair)) (val pair)))
            {}
            x)

    java.util.List
    (map map->kw-map x)

    true
    x))

;; ES client

(defn ^TransportClient es-connect-
  "Open an elasticsearch connection to a node."
  [node]
  (..  (PreBuiltTransportClient.
         (.. (Settings/builder)
             (put "cluster.name" "crate")
             (put "client.transport.sniff" false)
             (build))
         (make-array Class 0))
      (addTransportAddress (InetSocketTransportAddress.
                             (InetAddress/getByName (name node)) 44300))))

(defn es-connect
  "Opens an ES connection to a node, and ensures that it actually works"
  [node]
  (let [c (es-connect- node)]
    (util/with-retry [i 10]
      (-> c
          (.admin)
          (.cluster)
          (.prepareState)
          (.all)
          (.execute)
          (.actionGet))
      c
      (catch org.elasticsearch.client.transport.NoNodeAvailableException e
        (when (zero? i)
          (throw e))
        (info "Client not ready:" (type e))
        (Thread/sleep 5000)
        (retry (dec i))))))

(defn es-index!
  "Index a record"
  [^TransportClient client index type doc]
  (assert (:id doc))
  (let [res (-> client
                (.prepareIndex index type (str (:id doc)))
                (.setSource (json/generate-string doc))
                (.get))]; why not execute/actionGet?
    (when-not (= RestStatus/CREATED (.status res))
      (throw (RuntimeException. "Document not created")))
    res))

(defn es-get
  "Get a record by ID. Returns nil when record does not exist."
  [^TransportClient client index type id]
  (let [res (-> client
                (.prepareGet index type (str id))
                (.get))]
    (when (.isExists res)
      {:index   (.getIndex res)
       :type    (.getType res)
       :id      (.getId res)
       :version (.getVersion res)
       :source  (map->kw-map (.getSource res))})))

(defn es-search
  [^TransportClient client]
  (loop [results []
         scroll  (-> client
                     (.prepareSearch (into-array String []))
                     (.setScroll (TimeValue. 60000))
                     (.setSize 128)
                     (.execute)
                     (.actionGet))]
    (let [hits (.getHits (.getHits scroll))]
      (if (zero? (count hits))
        ; Done
        results

        (recur
          (->> hits
               seq
               (map (fn [hit]
                      {:id      (.id hit)
                       :version (.version hit)
                       :source  (map->kw-map (.getSource hit))}))
               (into results))
          (-> client
              (.prepareSearchScroll (.getScrollId scroll))
              (.setScroll (TimeValue. 60000))
              (.execute)
              (.actionGet)))))))

(def cratedb-spec {:dbtype "crate"
                   :dbname "test"
                   :classname "io.crate.client.jdbc.CrateDriver"
                   :user "crate"
                   :password ""
                   :port 55432})

(defn get-node-db-spec
  "Creates the db spec for the provided node"
  [node]
  (merge cratedb-spec {:host (name node)}))

(defn await-client
  "Takes a client and waits for it to become ready"
  [dbspec node test]
  (timeout 120000
           (throw (RuntimeException. (str (name node) " did not start up"))) 
           (with-retry []
             (j/query dbspec ["select name from sys.nodes"])
             dbspec
             (catch PSQLException e
               (Thread/sleep 1000)
               (retry)))))

;; DB

(defn install!
  "Install crate."
  [node crateVersion]
  (c/su
    (debian/install [:apt-transport-https])
    (debian/install-jdk8!)
    (c/cd "/tmp"
          (c/exec :wget "https://cdn.crate.io/downloads/apt/DEB-GPG-KEY-crate")
          (c/exec :apt-key :add "DEB-GPG-KEY-crate")
          (c/exec :rm "DEB-GPG-KEY-crate")
          (c/exec :wget (str "https://cdn.crate.io/downloads/apt/stable/pool/main/c/crate/crate_" crateVersion ".deb"))
          (c/exec :dpkg :-i (str "crate_" crateVersion ".deb"))
          (c/exec :apt-get :install :-f)
          (c/exec :rm (str "crate_" crateVersion ".deb")))
    (c/exec :update-rc.d :crate :disable))
  (info node "crate installed"))

(defn majority
  "n/2+1"
  [n]
  (-> n (/ 2) inc Math/floor long))

(defn configure!
  "Set up config files."
  [node test]
  (c/su
    (c/exec :echo
            (-> "crate.yml"
                io/resource
                slurp
                (str/replace "$NAME" (name node))
                (str/replace "$HOST" (.getHostAddress 
                                       (InetAddress/getByName (name node))))
                (str/replace "$N" (str (count (:nodes test))))
                (str/replace "$MAJORITY" (str (majority (count (:nodes test)))))
                (str/replace "$UNICAST_HOSTS"
                             (clojure.string/join ", " (map (fn [node] 
                                                              (str "\"" (name node) ":44300\"" ))
                                                            (:nodes test))))
                )                   
            :> "/etc/crate/crate.yml"))
  (info node "configured"))

(defn start!
  [node]
  (c/su
    (c/exec :service :crate :start)
    (info node "started")))

(defn db
  [crateVersion]
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! crateVersion)
        (configure! test)
        (start!)))

    (teardown! [_ test node]
      (cu/grepkill! "crate")
      (info node "killed")
      (c/exec :rm :-rf (c/lit "/var/log/crate/*"))
      (c/exec :rm :-rf (c/lit "/var/lib/crate/*")))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/crate/crate.log"])))

(defmacro with-errors
  "Unified error handling: takes an operation, evaluates body in a try/catch,
  and maps common exceptions to short errors."
  [op & body]
  `(try ~@body
        (catch PSQLException e#
          (cond
            (and (= 0 (.errorCode e#))
                 (re-find #"blocked by: \[.+no master\];" (str e#)))
            (assoc ~op :type :fail, :error :no-master)

            (and (= 0 (.errorCode e#))
                 (re-find #"document with the same primary key" (str e#)))
            (assoc ~op :type :fail, :error :duplicate-key)

            (and (= 0 (.errorCode e#))
                 (re-find #"rejected execution" (str e#)))
            (do ; Back off a bit
                (Thread/sleep 1000)
                (assoc ~op :type :info, :error :rejected-execution))

            :else
            (throw e#)))))
