(ns fiber-zookeeper-test
  (:require [clojure.test :refer :all]
            [fiber.zookeeper.core :refer :all]
            [co.paralleluniverse.pulsar.core :as p])
  (:import (org.apache.curator.test TestingServer)
           (org.apache.zookeeper ZooDefs$Ids CreateMode KeeperException$NodeExistsException)))

(def zk-port 2181)

(defn zk-server-fixture [f]
  (let [zk-server (TestingServer. zk-port)]
    (try
      (f)
      (finally (.stop zk-server)))))

(use-fixtures :each zk-server-fixture)

(defmacro in-fiber [f]
  `(p/join ~(cons `p/spawn-fiber f)))

; TESTS

(deftest create-node
  (testing "Create node from thread through zk sync API"
    (let [zk-client (connect (str "localhost:" zk-port))]
      (is (= "/node"
             (create zk-client "/node" nil ZooDefs$Ids/OPEN_ACL_UNSAFE CreateMode/EPHEMERAL)))
      (is (thrown? KeeperException$NodeExistsException
                   (create zk-client "/node" nil ZooDefs$Ids/OPEN_ACL_UNSAFE CreateMode/EPHEMERAL))))))

(deftest create-node-in-fiber
  (testing "Create node in fiber-mode through zk async API"
    (let [zk-client (connect (str "localhost:" zk-port))]
      (is (= "/nodes" (in-fiber (create zk-client "/nodes" nil ZooDefs$Ids/OPEN_ACL_UNSAFE CreateMode/EPHEMERAL))))
      (is (nil? (in-fiber (create zk-client "/nodes" nil ZooDefs$Ids/OPEN_ACL_UNSAFE CreateMode/EPHEMERAL)))))))
