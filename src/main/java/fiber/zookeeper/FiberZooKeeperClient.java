package fiber.zookeeper;

import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * Zookeeper client with fiber-blocking API.
 */
public final class FiberZooKeeperClient extends ZooKeeper {

  public FiberZooKeeperClient(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws IOException {
    super(connectString, sessionTimeout, watcher, canBeReadOnly);
  }

  /**
   * For docs look at {@link FiberZooKeeperAPI#create(ZooKeeper, String, byte[], List, CreateMode)}
   */
  @Suspendable
  @Override
  public String create(final String path, final byte data[], final List<ACL> acl,
                       final CreateMode createMode) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.create(this, path, data, acl, createMode);
    }
    return super.create(path, data, acl, createMode);
  }

  /**
   * For docs look at {@link FiberZooKeeperAPI#exists(ZooKeeper, String, boolean)}
   */
  @Suspendable
  @Override
  public Stat exists(final String path, final boolean watch) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.exists(this, path, watch);
    }
    return super.exists(path, watch);
  }

  /**
   * For docs look at {@link FiberZooKeeperAPI#exists(ZooKeeper, String, Watcher)}
   */
  @Suspendable
  @Override
  public Stat exists(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.exists(this, path, watcher);
    }
    return super.exists(path, watcher);
  }

  @Override
  @Suspendable
  public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.getData(this, path, watcher, stat);
    }
    return super.getData(path, watcher, stat);
  }

  @Override
  @Suspendable
  public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.getData(this, path, watch, stat);
    }
    return super.getData(path, watch, stat);
  }

  @Override
  @Suspendable
  public void delete(String path, int version) throws InterruptedException, KeeperException {
    if (Strand.isCurrentFiber()) {
      FiberZooKeeperAPI.delete(this, path, version);
    }
    super.delete(path, version);
  }

  @Suspendable
  @Override
  public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.setData(this, path, data, version);
    }
    return super.setData(path, data, version);
  }

  @Suspendable
  @Override
  public List<ACL> getACL(String path, Stat stat) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.getACL(this, path, stat);
    }
    return super.getACL(path, stat);
  }

  @Suspendable
  @Override
  public Stat setACL(String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.setACL(this, path, acl, version);
    }
    return super.setACL(path, acl, version);
  }

  @Suspendable
  @Override
  public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.getChildren(this, path, watcher);
    }
    return super.getChildren(path, watcher);
  }

  @Suspendable
  @Override
  public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.getChildren(this, path, watch);
    }
    return super.getChildren(path, watch);
  }

  @Suspendable
  @Override
  public List<String> getChildren(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.getChildren(this, path, watcher, stat);
    }
    return super.getChildren(path, watcher, stat);
  }

  @Suspendable
  @Override
  public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
    if (Strand.isCurrentFiber()) {
      return FiberZooKeeperAPI.getChildren(this, path, watch, stat);
    }
    return super.getChildren(path, watch, stat);
  }
}
