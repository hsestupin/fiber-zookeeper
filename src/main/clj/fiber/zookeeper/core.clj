(ns fiber.zookeeper.core
  (:import (fiber.zookeeper FiberZooKeeperAPI)
           (org.apache.zookeeper ZooKeeper CreateMode Watcher)
           (java.util List)))

(defn ^Watcher do-nothing-watcher []
  (reify Watcher
    (process [_ __]
      )))

(defn connect
  ([connect-string]
   (connect connect-string 1000))
  ([connect-string session-timeout]
   (connect connect-string session-timeout (do-nothing-watcher)))
  ([connect-string session-timeout watcher]
   (ZooKeeper. connect-string session-timeout watcher)))

(defn ^String create
  [^ZooKeeper zk ^String path data ^List acl ^CreateMode create-mode]
  (FiberZooKeeperAPI/create zk path data acl create-mode nil))

(comment
  (clojure.core/require '(co.paralleluniverse.pulsar [core :as pc]))

  (def zk (ZooKeeper. "localhost:2181" 3000 nil))
  (pc/join (pc/spawn-fiber create
                           zk
                           "/nodes"
                           nil
                           org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                           CreateMode/EPHEMERAL))

  )
