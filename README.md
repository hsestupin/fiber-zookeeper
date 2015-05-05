# fiber-zookeeper
Native ZooKeeper async API wrapped in Quasar lightweight fibers.

# Rationale
The rationale is nicely explained at this blogpost http://blog.paralleluniverse.co/2014/08/12/noasync/

Briefly this small library attemts to provide all of the beauty of synchronous API without thread-blocking. Under the hood it utilizes Zookeeper async API and presents fiber-blocking synchronous API to the user. As a result you gain all benefits of both  approaches:
* readability of synchronous API
* performance of async

There are 2 implementations right now - for java and clojure. 

## License

Copyright (C) 2015 Sergey Stupin

Distributed under the Eclipse Public License, the same as Clojure.
