package fiber.zookeeper;

import co.paralleluniverse.fibers.FiberAsync;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Static methods which wrap ZooKeeper async API into fiber-blocking one.
 */
public class FiberZooKeeperAPI {

  /**
   * Fiber-blocking wrapper for {@link ZooKeeper#create(String, byte[], List, CreateMode, AsyncCallback.StringCallback, Object)}.
   * <p/>
   * This method can be even called from a thread not a fiber. But in that case ZooKeeper thread-blocking API
   * will be utilized behind the scene - {@link ZooKeeper#create(String, byte[], List, CreateMode)}
   * <p/>
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
   * <p/>
   * This method can be even called from a thread not a fiber. But in that case ZooKeeper thread-blocking API
   * will be utilized behind the scene - {@link ZooKeeper#exists(String, boolean)}
   * <p/>
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


  private abstract static class StringFiberAsync extends AbstractZooKeeperFiberAsync<String>
      implements AsyncCallback.StringCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      asyncCompleted(name);
    }

  }

  private abstract static class StatFiberAsync extends AbstractZooKeeperFiberAsync<Stat>
      implements AsyncCallback.StatCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      asyncCompleted(stat);
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
