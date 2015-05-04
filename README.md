# fiber-zookeeper
Native ZooKeeper async API wrapped in Quasar lightweight fibers. There are 2 implementations - for java and clojure.

# Rationale
The rationale is nicely explained at this blogpost http://blog.paralleluniverse.co/2014/08/12/noasync/
Shortly this small library attemts to provide all of the beauty of synchronous API without thread-blocking. Under the hood it utilizes Zookeeper async API and presents fiber-blocking synchronous API to the user. 
