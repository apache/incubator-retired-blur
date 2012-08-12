package com.nearinfinity.blur.manager.clusterstatus;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.utils.BlurUtil;

public class ZookeeperClusterStatusTest {

  private static final String DEFAULT = "default";

  private static final Log LOG = LogFactory.getLog(ZookeeperClusterStatusTest.class);

  private static Thread serverThread;
  private static String connectionString = "localhost:21810";
  private ZooKeeper zooKeeper;

  private ZookeeperClusterStatus clusterStatus;

  @BeforeClass
  public static void setupOnce() throws InterruptedException, IOException, KeeperException {
    rm(new File("./tmp/zk_test"));
    serverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        QuorumPeerMain.main(new String[] { "./src/test/resources/test_zoo.cfg" });
      }
    });
    serverThread.start();
    for (int i = 0; i < 10; i++) {
      Thread.sleep(1000);
      try {
        ZooKeeper zk = new ZooKeeper(connectionString, 30000, new Watcher() {
          @Override
          public void process(WatchedEvent event) {

          }
        });
        zk.close();
        break;
      } catch (IOException e) {
        LOG.error(e);
      }
    }
  }

  private static void rm(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }

  @AfterClass
  public static void teardownOnce() {
    serverThread.interrupt();
  }

  @Before
  public void setup() throws KeeperException, InterruptedException, IOException {
    zooKeeper = new ZooKeeper(connectionString, 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
    BlurUtil.setupZookeeper(zooKeeper, DEFAULT);
    setSafeModeInPast();
    clusterStatus = new ZookeeperClusterStatus(zooKeeper);
  }
  
  @After
  public void teardown() throws InterruptedException {
    zooKeeper.close();
    clusterStatus.close();
  }

  @Test
  public void testGetClusterList() {
    List<String> clusterList = clusterStatus.getClusterList();
    assertEquals(Arrays.asList(DEFAULT), clusterList);
  }

  @Test
  public void testSafeModeNotSet() throws KeeperException, InterruptedException {
    assertFalse(clusterStatus.isInSafeMode(false, DEFAULT));
    new WaitForAnswerToBeCorrect(20L) {
      @Override
      public Object run() {
        return clusterStatus.isInSafeMode(true, DEFAULT);
      }
    }.test(false);
  }

  @Test
  public void testSafeModeSetInPast() throws KeeperException, InterruptedException {
    setSafeModeInPast();
    assertFalse(clusterStatus.isInSafeMode(false, DEFAULT));
    new WaitForAnswerToBeCorrect(20L) {
      @Override
      public Object run() {
        return clusterStatus.isInSafeMode(true, DEFAULT);
      }
    }.test(false);
  }

  @Test
  public void testSafeModeSetInFuture() throws KeeperException, InterruptedException {
    setSafeModeInFuture();
    assertTrue(clusterStatus.isInSafeMode(false, DEFAULT));
    new WaitForAnswerToBeCorrect(20L) {
      @Override
      public Object run() {
        return clusterStatus.isInSafeMode(true, DEFAULT);
      }
    }.test(true);
  }

  public abstract class WaitForAnswerToBeCorrect {

    private long totalWaitTimeNanos;

    public WaitForAnswerToBeCorrect(long totalWaitTimeMs) {
      this.totalWaitTimeNanos = TimeUnit.MILLISECONDS.toNanos(totalWaitTimeMs);
    }

    public abstract Object run();

    public void test(Object o) {
      long start = System.nanoTime();
      while (true) {
        Object object = run();
        if (object.equals(o) || object == o) {
          return;
        }
        long now = System.nanoTime();
        if (now - start > totalWaitTimeNanos) {
          fail();
        }
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          fail(e.getMessage());
        }
      }
    }
  }

  private void setSafeModeInPast() throws KeeperException, InterruptedException {
    String blurSafemodePath = ZookeeperPathConstants.getSafemodePath(DEFAULT);
    Stat stat = zooKeeper.exists(blurSafemodePath, false);
    byte[] data = Long.toString(System.currentTimeMillis() - 60000).getBytes();
    if (stat == null) {
      zooKeeper.create(blurSafemodePath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    zooKeeper.setData(blurSafemodePath, data, -1);
  }

  private void setSafeModeInFuture() throws KeeperException, InterruptedException {
    String blurSafemodePath = ZookeeperPathConstants.getSafemodePath(DEFAULT);
    Stat stat = zooKeeper.exists(blurSafemodePath, false);
    byte[] data = Long.toString(System.currentTimeMillis() + 60000).getBytes();
    if (stat == null) {
      zooKeeper.create(blurSafemodePath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    zooKeeper.setData(blurSafemodePath, data, -1);
  }

}
