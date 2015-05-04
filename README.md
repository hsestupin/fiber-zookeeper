# fiber-zookeeper
Native ZooKeeper async API wrapped in Quasar lightweight fibers.

# Rationale
The rationale is nicely explained at this blogpost http://blog.paralleluniverse.co/2014/08/12/noasync/

Briefly this small library attemts to provide all of the beauty of synchronous API without thread-blocking. Under the hood it utilizes Zookeeper async API and presents fiber-blocking synchronous API to the user. There are 2 implementations - for java and clojure.

## License

Copyright (C) 2015 Sergey Stupin

Distributed under the Eclipse Public License, the same as Clojure.
