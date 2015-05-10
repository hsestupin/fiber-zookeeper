(ns fiber-zookeeper-test
  (:require [clojure.test :refer :all]
            [fiber.zookeeper.core :refer :all]
            [co.paralleluniverse.pulsar.core :as p])
  (:import (org.apache.curator.test TestingServer)
           (org.apache.zookeeper KeeperException$NodeExistsException)
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
    (with-open [zk-client (connect (str "localhost:" zk-port))]
      (let [path "/node"]
        (is (= path (create zk-client path)))
        (is (thrown? KeeperException$NodeExistsException (create zk-client path)))))))

(deftest create-node-in-fiber
  (testing "Create node in fiber-mode through zk async API"
    (with-open [zk-client (connect (str "localhost:" zk-port))]
      (let [path "/node"]
        (is (= path (in-fiber (create zk-client path))))
        (is (nil? (in-fiber (create zk-client path))))))))

(deftest exists-node
  (testing "Checking node for existence from thread through zk sync API"
    (with-open [zk-client (connect (str "localhost:" zk-port))]
      (let [path "/node"
            watcher (do-nothing-watcher)]
        (is (nil? (exists zk-client path)))
        (is (nil? (exists zk-client path watcher)))

        (create zk-client path)
        (let [^Stat stat1 (exists zk-client path)
              ^Stat stat2 (exists zk-client path watcher)]
          (is (= stat1 stat2))
          (is (zero? (.getVersion stat1)))
          (is (zero? (.getDataLength stat1)))
          (is (zero? (.getNumChildren stat1))))))))

(deftest exists-node-in-fiber
  (testing "Checking node in fiber-mode through zk async API"
    (with-open [zk-client (connect (str "localhost:" zk-port))]
      (let [path "/node"
            watcher (do-nothing-watcher)]
        (is (nil? (in-fiber (exists zk-client path))))
        (is (nil? (in-fiber (exists zk-client path watcher))))

        (create zk-client path)
        (let [^Stat stat1 (in-fiber (exists zk-client path))
              ^Stat stat2 (in-fiber (exists zk-client path watcher))]
          (is (= stat1 stat2))
          (is (zero? (.getVersion stat1)))
          (is (zero? (.getDataLength stat1)))
          (is (zero? (.getNumChildren stat1))))))))
