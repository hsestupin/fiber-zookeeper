(defproject fiber-zookeeper "0.1.0"
  :description "Native ZooKeeper async API wrapped in Quasar lightweight fibers"
  :min-lein-version "2.0.0"
  :url "https://github.com/hsestupin/fiber-zookeeper"
  :license {:name "Apache License, Version 2.0"
            :url  "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [co.paralleluniverse/pulsar "0.7.4"]
                 [org.apache.zookeeper/zookeeper "3.4.6"]]
  :source-paths ["src/main/clj"]
  :test-paths ["src/test/clj"]
  :java-source-paths ["src/main/java"]
  ;:javac-options ["-target" "1.8" "-source" "1.8"]
  :java-agents [[co.paralleluniverse/quasar-core "0.7.4"]]

  :profiles {:dev {:dependencies [[org.apache.curator/curator-test "2.7.1"]]}})
