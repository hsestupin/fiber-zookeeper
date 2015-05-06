package fiber.zookeeper;

import co.paralleluniverse.fibers.Suspendable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

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
   * For docs look at {@link FiberZooKeeperAPI#create(ZooKeeper, String, byte[], List, CreateMode, Object)}
   */
  @Suspendable
  public String create(final String path, final byte data[], final List<ACL> acl,
                       final CreateMode createMode, final Object ctx) throws KeeperException, InterruptedException {
    return FiberZooKeeperAPI.create(zk, path, data, acl, createMode, ctx);
  }

}
