(ns fiber.zookeeper.core
  (:import (fiber.zookeeper FiberZooKeeperAPI)
           (org.apache.zookeeper ZooKeeper CreateMode)
           (java.util List)))

(defn ^String create
  [^ZooKeeper zk
   ^String path
   data                                                     ;
   ^List acl
   ^CreateMode create-mode]
  (FiberZooKeeperAPI/create zk path data acl create-mode nil))

(comment
  (require '(co.paralleluniverse.pulsar [core :as pc]))

  (def f (pc/spawn-fiber create
                         zk
                         "/nodes"
                         nil
                         org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                         CreateMode/EPHEMERAL))

  (def zk (ZooKeeper. "localhost:2181" 3000 nil)))