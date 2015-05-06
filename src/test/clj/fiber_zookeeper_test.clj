(ns fiber-zookeeper-test
  (:require [clojure.test :refer :all]
            [fiber.zookeeper.core :refer :all]
            [co.paralleluniverse.pulsar.core :as p])
  (:import (org.apache.curator.test TestingServer)
           (org.apache.zookeeper ZooDefs$Ids CreateMode KeeperException$NodeExistsException)
           (org.apache.zookeeper.data Stat)))

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
      (is (= "/node" (create zk-client "/node")))
      (is (thrown? KeeperException$NodeExistsException (create zk-client "/node"))))))

(deftest create-node-in-fiber
  (testing "Create node in fiber-mode through zk async API"
    (let [zk-client (connect (str "localhost:" zk-port))
          path "/node"]
      (is (= path (in-fiber (create zk-client path))))
      (is (nil? (in-fiber (create zk-client path)))))))

(deftest exists-node
  (testing "Checking node for existence from thread through zk sync API"
    (let [zk-client (connect (str "localhost:" zk-port))
          path "/node"]
      (is (nil? (exists zk-client path)))

      (create zk-client path)
      (let [^Stat stat (exists zk-client path)]
        (is (zero? (.getVersion stat)))
        (is (zero? (.getDataLength stat)))
        (is (zero? (.getNumChildren stat)))))))

(deftest exists-node-in-fiber
  (testing "Checking node in fiber-mode through zk async API"
    (let [zk-client (connect (str "localhost:" zk-port))
          path "/node"]
      (is (nil? (in-fiber (exists zk-client path))))

      (create zk-client path)
      (let [^Stat stat (in-fiber (exists zk-client path))]
        (is (zero? (.getVersion stat)))
        (is (zero? (.getDataLength stat)))
        (is (zero? (.getNumChildren stat)))))))
