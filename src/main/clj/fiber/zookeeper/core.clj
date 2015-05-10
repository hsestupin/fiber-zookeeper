(ns fiber.zookeeper.core
  (:import (fiber.zookeeper FiberZooKeeperAPI)
           (org.apache.zookeeper ZooKeeper CreateMode Watcher ZooDefs$Ids)
           (java.util List)
           (org.apache.zookeeper.data Stat)))

(defn ^Watcher do-nothing-watcher []
  (reify Watcher
    (process [_ __]
      )))

(defn connect
  ([connect-string]
   (connect connect-string 2000))
  ([connect-string session-timeout]
   (connect connect-string session-timeout (do-nothing-watcher)))
  ([connect-string session-timeout watcher]
   (ZooKeeper. connect-string session-timeout watcher)))

(defn ^String create
  ([^ZooKeeper zk ^String path]
   (create zk path nil ZooDefs$Ids/OPEN_ACL_UNSAFE CreateMode/EPHEMERAL))
  ([^ZooKeeper zk ^String path data ^List acl ^CreateMode create-mode]
   (FiberZooKeeperAPI/create zk path data acl create-mode)))

(defmulti ^Stat exists
          (fn
            ([_ _ watcher]
             (class watcher))
            ([_ _] nil)))

(defmethod exists Boolean
  [^ZooKeeper zk ^String path ^Boolean watch]
  (FiberZooKeeperAPI/exists zk path watch))

(defmethod exists Watcher
  [^ZooKeeper zk ^String path ^Watcher watcher]
  (FiberZooKeeperAPI/exists zk path watcher))

(defmethod exists :default
  [^ZooKeeper zk ^String path]
  (FiberZooKeeperAPI/exists zk path false))

(comment
  (in-ns 'fiber.zookeeper.core)
  (clojure.core/require '(co.paralleluniverse.pulsar [core :as pc]))

  (def zk (connect
            "localhost:2181"
            3000
            (reify Watcher
              (process [_ event]
                (print "OMG: " event)))))

  (pc/join (pc/spawn-fiber create
                           zk
                           "/nodes"
                           nil
                           ZooDefs$Ids/OPEN_ACL_UNSAFE
                           CreateMode/EPHEMERAL))

  (pc/join (pc/spawn-fiber exists
                           zk
                           "/nodes"
                           false))

  (pc/join (pc/spawn-fiber exists
                           zk
                           "/nodes"))

  (pc/join (pc/spawn-fiber exists
                           zk
                           "/nodes"
                           (reify Watcher
                             (process [_ event]
                               (print "OMG2: " event)))))

  )
