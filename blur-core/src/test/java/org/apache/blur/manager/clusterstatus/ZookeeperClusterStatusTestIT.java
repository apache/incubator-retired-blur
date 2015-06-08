package org.apache.blur.manager.clusterstatus;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.SuiteCluster;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.zookeeper.ZooKeeperClient;
import org.apache.blur.zookeeper.ZooKeeperLockManager;
import org.apache.blur.zookeeper.ZookeeperPathConstants;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZookeeperClusterStatusTestIT {

  private static final String TEST = "test";
  private static final String DEFAULT = "default";

  private static final Log LOG = LogFactory.getLog(ZookeeperClusterStatusTestIT.class);
  private ZooKeeper zooKeeper1;
  private ZooKeeper zooKeeper2;
  private ZookeeperClusterStatus clusterStatus1;
  private ZookeeperClusterStatus clusterStatus2;

  public static class QuorumPeerMainRun extends QuorumPeerMain {
    @Override
    public void initializeAndRun(String[] args) throws ConfigException, IOException {
      super.initializeAndRun(args);
    }
  }

  @BeforeClass
  public static void startup() throws IOException, BlurException, TException {
    SuiteCluster.setupMiniCluster(ZookeeperClusterStatusTestIT.class);
  }

  @AfterClass
  public static void shutdown() throws IOException {
    SuiteCluster.shutdownMiniCluster(ZookeeperClusterStatusTestIT.class);
  }

  @Before
  public void setup() throws KeeperException, InterruptedException, IOException {
    zooKeeper1 = new ZooKeeperClient(SuiteCluster.getZooKeeperConnStr(ZookeeperClusterStatusTestIT.class), 30000,
        new Watcher() {
          @Override
          public void process(WatchedEvent event) {

          }
        });
    BlurUtil.setupZookeeper(zooKeeper1, DEFAULT);
    zooKeeper2 = new ZooKeeperClient(SuiteCluster.getZooKeeperConnStr(ZookeeperClusterStatusTestIT.class), 30000,
        new Watcher() {
          @Override
          public void process(WatchedEvent event) {

          }
        });
    BlurUtil.setupZookeeper(zooKeeper1, DEFAULT);
    BlurUtil.setupZookeeper(zooKeeper2, DEFAULT);
    clusterStatus1 = new ZookeeperClusterStatus(zooKeeper1);
    clusterStatus2 = new ZookeeperClusterStatus(zooKeeper2);
  }

  @After
  public void teardown() throws InterruptedException, KeeperException {
    clusterStatus1.close();
    clusterStatus2.close();
    rmr(zooKeeper1, "/blur");
    zooKeeper1.close();
    zooKeeper2.close();
  }

  private static void rmr(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {
    List<String> children = zooKeeper.getChildren(path, false);
    for (String c : children) {
      rmr(zooKeeper, path + "/" + c);
    }
    zooKeeper.delete(path, -1);
  }

  @Test
  public void testGetClusterList() {
    LOG.warn("testGetClusterList");
    List<String> clusterList = clusterStatus2.getClusterList(false);
    assertEquals(Arrays.asList(DEFAULT), clusterList);
  }

  @Test
  public void testSafeModeNoCache() throws KeeperException, InterruptedException {
    String safemodePath = ZookeeperPathConstants.getSafemodePath(DEFAULT);
    ZooKeeperLockManager zooKeeperLockManager = new ZooKeeperLockManager(zooKeeper1, safemodePath);
    zooKeeperLockManager.lock(DEFAULT);
    assertTrue(clusterStatus2.isInSafeMode(false, DEFAULT));
    zooKeeperLockManager.unlock(DEFAULT);
    assertFalse(clusterStatus2.isInSafeMode(false, DEFAULT));
  }

  @Test
  public void testSafeModeCache() throws KeeperException, InterruptedException {
    String safemodePath = ZookeeperPathConstants.getSafemodePath(DEFAULT);
    ZooKeeperLockManager zooKeeperLockManager = new ZooKeeperLockManager(zooKeeper1, safemodePath);
    zooKeeperLockManager.lock(DEFAULT);
    new WaitForAnswerToBeCorrect(20L) {
      @Override
      public Object run() {
        return clusterStatus2.isInSafeMode(false, DEFAULT);
      }
    }.test(true);
    zooKeeperLockManager.unlock(DEFAULT);
    new WaitForAnswerToBeCorrect(20L) {
      @Override
      public Object run() {
        return clusterStatus2.isInSafeMode(false, DEFAULT);
      }
    }.test(false);
  }

  @Test
  public void testGetClusterNoTable() {
    LOG.warn("testGetCluster");
    assertNull(clusterStatus2.getCluster(false, TEST));
    assertNull(clusterStatus2.getCluster(true, TEST));
  }

  @Test
  public void testGetClusterTable() throws KeeperException, InterruptedException {
    LOG.warn("testGetCluster");
    createTable(TEST);
    assertEquals(DEFAULT, clusterStatus2.getCluster(false, TEST));
    new WaitForAnswerToBeCorrect(20L) {
      @Override
      public Object run() {
        return clusterStatus2.getCluster(true, TEST);
      }
    }.test(DEFAULT);
  }

  @Test
  public void testGetTableList() throws KeeperException, InterruptedException {
    testGetClusterTable();
    assertEquals(Arrays.asList(TEST), clusterStatus2.getTableList(false));
  }

  @Test
  public void testIsEnabledNoTable() {
    try {
      clusterStatus1.isEnabled(false, DEFAULT, "notable");
      fail("should throw exception.");
    } catch (RuntimeException e) {

    }
    try {
      clusterStatus1.isEnabled(true, DEFAULT, "notable");
      fail("should throw exception.");
    } catch (RuntimeException e) {

    }
  }

  @Test
  public void testIsEnabledDisabledTable() throws KeeperException, InterruptedException {
    createTable("disabledtable", false);
    assertFalse(clusterStatus2.isEnabled(false, DEFAULT, "disabledtable"));
    assertFalse(clusterStatus2.isEnabled(true, DEFAULT, "disabledtable"));
  }

  @Test
  public void testIsEnabledEnabledTable() throws KeeperException, InterruptedException {
    createTable("enabledtable", true);
    assertTrue(clusterStatus2.isEnabled(false, DEFAULT, "enabledtable"));

    new WaitForAnswerToBeCorrect(20L) {
      @Override
      public Object run() {
        return clusterStatus2.isEnabled(true, DEFAULT, "enabledtable");
      }
    }.test(true);
  }

  @Test
  public void testDisablingTableNoCache() throws KeeperException, InterruptedException {
    createTable(TEST);
    assertTrue(clusterStatus2.isEnabled(false, DEFAULT, TEST));
    clusterStatus1.disableTable(DEFAULT, TEST);
    assertFalse(clusterStatus2.isEnabled(false, DEFAULT, TEST));
  }

  @Test
  public void testDisablingTableCache() throws KeeperException, InterruptedException {
    createTable(TEST);
    assertTrue(clusterStatus2.isEnabled(true, DEFAULT, TEST));
    clusterStatus1.disableTable(DEFAULT, TEST);
    new WaitForAnswerToBeCorrect(20L) {
      @Override
      public Object run() {
        return clusterStatus2.isEnabled(true, DEFAULT, TEST);
      }
    }.test(false);
  }

  @Test
  public void testEnablingTableNoCache() throws KeeperException, InterruptedException {
    createTable(TEST, false);
    assertFalse(clusterStatus2.isEnabled(false, DEFAULT, TEST));
    clusterStatus1.enableTable(DEFAULT, TEST);
    assertTrue(clusterStatus2.isEnabled(false, DEFAULT, TEST));
  }

  @Test
  public void testEnablingTableCache() throws KeeperException, InterruptedException {
    createTable(TEST, false);
    assertFalse(clusterStatus2.isEnabled(true, DEFAULT, TEST));
    clusterStatus1.enableTable(DEFAULT, TEST);
    new WaitForAnswerToBeCorrect(20L) {
      @Override
      public Object run() {
        return clusterStatus2.isEnabled(true, DEFAULT, TEST);
      }
    }.test(true);
  }

  private void createTable(String name) throws KeeperException, InterruptedException {
    createTable(name, true);
  }

  private void createTable(String name, boolean enabled) throws KeeperException, InterruptedException {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(name);
    tableDescriptor.setTableUri("./target/tmp/zk_test_hdfs");
    tableDescriptor.setEnabled(enabled);
    clusterStatus1.createTable(tableDescriptor);
    if (enabled) {
      clusterStatus1.enableTable(tableDescriptor.getCluster(), name);
    }
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

}
