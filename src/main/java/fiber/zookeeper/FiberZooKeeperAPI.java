package fiber.zookeeper;

import co.paralleluniverse.fibers.FiberAsync;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Static methods which wrap ZooKeeper async API into fiber-blocking one.
 */
public enum FiberZooKeeperAPI {
  ;

  /**
   * Fiber-blocking wrapper for {@link ZooKeeper#create(String, byte[], List, CreateMode, AsyncCallback.StringCallback, Object)}.
   * <p>
   * This method can be even called from a java thread. But in that case ZooKeeper thread-blocking API
   * will be utilized behind the scene - {@link ZooKeeper#create(String, byte[], List, CreateMode)}
   * <p>
   *
   * @return the actual path of the created node or null if node was not created due to some reasons
   * @throws org.apache.zookeeper.KeeperException.NodeExistsException if method is invoked from a thread (not from a fiber) and node
   *                                                                  already exists
   */
  @Suspendable
  public static String create(final ZooKeeper zk, final String path, final byte data[], final List<ACL> acl,
                              final CreateMode createMode) throws KeeperException, InterruptedException {
    return new StringFiberAsync() {

      @Override
      protected void requestAsync() {
        zk.create(path, data, acl, createMode, this, null);
      }

      @Override
      protected String requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.create(path, data, acl, createMode);
      }
    }.run();
  }

  /**
   * Fiber-blocking wrapper for {@link ZooKeeper#exists(String, boolean, AsyncCallback.StatCallback, Object)}.
   * <p>
   * This method can be even called from a java thread. But in that case ZooKeeper thread-blocking API
   * will be utilized behind the scene - {@link ZooKeeper#exists(String, boolean)}
   * <p>
   *
   * @return the stat of the node of the given path; return null if no such a
   * node exists.
   */
  @Suspendable
  public static Stat exists(final ZooKeeper zk, final String path, final boolean watch) throws KeeperException, InterruptedException {
    return new StatFiberAsync() {

      @Override
      protected void requestAsync() {
        zk.exists(path, watch, this, null);
      }

      @Override
      protected Stat requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.exists(path, watch);
      }
    }.run();
  }

  /**
   * Fiber-blocking wrapper for {@link ZooKeeper#exists(String, Watcher, AsyncCallback.StatCallback, Object)}.
   * <p>
   * This method can be even called from a java thread. But in that case ZooKeeper thread-blocking API
   * will be utilized behind the scene - {@link ZooKeeper#exists(String, Watcher)}
   * <p>
   *
   * @return the stat of the node of the given path; return null if no such a
   * node exists.
   */
  @Suspendable
  public static Stat exists(final ZooKeeper zk, final String path, final Watcher watcher) throws KeeperException, InterruptedException {
    return new StatFiberAsync() {

      @Override
      protected void requestAsync() {
        zk.exists(path, watcher, this, null);
      }

      @Override
      protected Stat requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.exists(path, watcher);
      }
    }.run();
  }

  public static byte[] getData(ZooKeeper zk, String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
    return new DataFiberAsync() {

      @Override
      protected void requestAsync() {
        zk.getData(path, watcher, this, null);
      }

      @Override
      protected byte[] requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.getData(path, watcher, stat);
      }
    }.run();
  }

  public static byte[] getData(ZooKeeper zk, String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
    return new DataFiberAsync() {

      @Override
      protected void requestAsync() {
        zk.getData(path, watch, this, null);
      }

      @Override
      protected byte[] requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.getData(path, watch, stat);
      }
    }.run();
  }

  public static void delete(ZooKeeper zk, String path, int version) throws KeeperException, InterruptedException {
    new VoidFiberAsync() {

      @Override
      protected void requestAsync() {
        zk.delete(path, version, this, null);
      }

      @Override
      protected Void requestSync() throws KeeperException, InterruptedException, ExecutionException {
        zk.delete(path, version);
        return null;
      }
    }.run();
  }

  public static Stat setData(ZooKeeper zk, String path, byte[] data, int version) throws KeeperException, InterruptedException {
    return new StatFiberAsync() {

      @Override
      protected void requestAsync() {
        zk.setData(path, data, version, this, null);
      }

      @Override
      protected Stat requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.setData(path, data, version);
      }
    }.run();
  }

  public static List<ACL> getACL(ZooKeeper zk, String path, Stat stat) throws KeeperException, InterruptedException {
    return new ACLFiberAsync() {

      @Override
      protected void requestAsync() {
        zk.getACL(path, stat, this, null);
      }

      @Override
      protected List<ACL> requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.getACL(path, stat);
      }
    }.run();
  }

  public static Stat setACL(ZooKeeper zk, String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
    return new StatFiberAsync() {

      @Override
      protected void requestAsync() {
        zk.setACL(path, acl, version, this, null);
      }

      @Override
      protected Stat requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.setACL(path, acl, version);
      }
    }.run();
  }

  public static List<String> getChildren(ZooKeeper zk, String path, Watcher watcher) throws KeeperException, InterruptedException {
    return new ChildrenFiberAsync() {

      @Override
      protected void requestAsync() {
        zk.getChildren(path, watcher, this, null);
      }

      @Override
      protected List<String> requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.getChildren(path, watcher);
      }
    }.run();
  }

  public static List<String> getChildren(ZooKeeper zk, String path, boolean watch) throws KeeperException, InterruptedException {
    return new ChildrenFiberAsync() {

      @Override
      protected void requestAsync() {
        zk.getChildren(path, watch, this, null);
      }

      @Override
      protected List<String> requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.getChildren(path, watch);
      }
    }.run();
  }

  public static List<String> getChildren(ZooKeeper zk, String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
    return new Children2FiberAsync() {

      @Override
      protected void requestAsync() {
        zk.getChildren(path, watcher, this, null);
      }

      @Override
      protected List<String> requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.getChildren(path, watcher, stat);
      }
    }.run();
  }

  public static List<String> getChildren(ZooKeeper zk, String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
    return new Children2FiberAsync() {

      @Override
      protected void requestAsync() {
        zk.getChildren(path, watch, this, null);
      }

      @Override
      protected List<String> requestSync() throws KeeperException, InterruptedException, ExecutionException {
        return zk.getChildren(path, watch, stat);
      }
    }.run();
  }

  private abstract static class ChildrenFiberAsync extends AbstractZooKeeperFiberAsync<List<String>>
      implements AsyncCallback.ChildrenCallback {
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
      asyncCompleted(children);
    }
  }

  private abstract static class Children2FiberAsync extends AbstractZooKeeperFiberAsync<List<String>>
      implements AsyncCallback.Children2Callback {

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {

    }
  }

  private abstract static class VoidFiberAsync extends AbstractZooKeeperFiberAsync<Void>
      implements AsyncCallback.VoidCallback {

    @Override
    public void processResult(int rc, String path, Object ctx) {
      asyncCompleted(null);
    }
  }

  private abstract static class StringFiberAsync extends AbstractZooKeeperFiberAsync<String>
      implements AsyncCallback.StringCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      asyncCompleted(name);
    }

  }

  private abstract static class ACLFiberAsync extends AbstractZooKeeperFiberAsync<List<ACL>>
      implements AsyncCallback.ACLCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, List<ACL> acl, Stat stat) {
      asyncCompleted(acl);
    }
  }

  private abstract static class StatFiberAsync extends AbstractZooKeeperFiberAsync<Stat>
      implements AsyncCallback.StatCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      asyncCompleted(stat);
    }
  }

  private abstract static class DataFiberAsync extends AbstractZooKeeperFiberAsync<byte[]>
      implements AsyncCallback.DataCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      asyncCompleted(data);
    }
  }

  private abstract static class AbstractZooKeeperFiberAsync<T> extends FiberAsync<T, KeeperException> {

    @Override
    @Suspendable
    public T run() throws KeeperException, InterruptedException {
      try {
        return super.run();
      } catch (SuspendExecution e) {
        throw new AssertionError();
      }
    }

    @Override
    @Suspendable
    public T run(long timeout, TimeUnit unit) throws KeeperException, InterruptedException, TimeoutException {
      try {
        return super.run(timeout, unit);
      } catch (SuspendExecution e) {
        throw new AssertionError();
      }
    }

  }

}
