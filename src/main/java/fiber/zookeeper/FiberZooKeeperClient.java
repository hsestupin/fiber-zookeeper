package fiber.zookeeper;

import co.paralleluniverse.fibers.Suspendable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * Zookeeper client with fiber-blocking API.
 */
public class FiberZooKeeperClient {


  private final ZooKeeper zk;

  public FiberZooKeeperClient(ZooKeeper zk) {
    this.zk = zk;
  }

  /**
   * For docs look at {@link FiberZooKeeperAPI#create(ZooKeeper, String, byte[], List, CreateMode)}
   */
  @Suspendable
  public String create(final String path, final byte data[], final List<ACL> acl,
                       final CreateMode createMode) throws KeeperException, InterruptedException {
    return FiberZooKeeperAPI.create(zk, path, data, acl, createMode);
  }

  /**
   * For docs look at {@link FiberZooKeeperAPI#exists(ZooKeeper, String, boolean)}
   */
  @Suspendable
  public static Stat exists(final ZooKeeper zk, final String path, final boolean watch) throws KeeperException, InterruptedException {
    return FiberZooKeeperAPI.exists(zk, path, watch);
  }

  /**
   * For docs look at {@link FiberZooKeeperAPI#exists(ZooKeeper, String, Watcher)}
   */
  @Suspendable
  public static Stat exists(final ZooKeeper zk, final String path, final Watcher watcher) throws KeeperException, InterruptedException {
    return FiberZooKeeperAPI.exists(zk, path, watcher);
  }
}
