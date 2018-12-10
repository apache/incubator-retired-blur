package org.apache.blur.manager.indexserver;

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.SuiteCluster;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.zookeeper.ZooKeeperClient;
import org.apache.blur.zookeeper.ZooKeeperLockManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SafeModeTestIT {

  @BeforeClass
  public static void startup() throws IOException, BlurException, TException {
    SuiteCluster.setupMiniCluster(SafeModeTestIT.class);
  }

  @AfterClass
  public static void shutdown() throws IOException {
    SuiteCluster.shutdownMiniCluster(SafeModeTestIT.class);
  }

  private ZooKeeper zk;

  @Before
  public void setup() throws IOException {
    zk = new ZooKeeperClient(SuiteCluster.getZooKeeperConnStr(SafeModeTestIT.class), 20000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
  }

  @After
  public void teardown() throws KeeperException, InterruptedException {
    rm(zk, "/testing");
    zk.close();
  }

  @Test
  public void testBasicStartup() throws IOException, InterruptedException, KeeperException {
    List<AtomicReference<Throwable>> errors = new ArrayList<AtomicReference<Throwable>>();
    List<AtomicLong> timeRegisteredLst = new ArrayList<AtomicLong>();
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < 32; i++) {
      AtomicReference<Throwable> ref = new AtomicReference<Throwable>();
      AtomicLong timeRegistered = new AtomicLong();
      errors.add(ref);
      timeRegisteredLst.add(timeRegistered);
      threads.add(startThread(zk, "node" + i, ref, timeRegistered));
      Thread.sleep(100);
    }

    boolean alive = true;
    while (alive) {
      alive = false;
      for (Thread t : threads) {
        t.join(1000);
        if (t.isAlive()) {
          System.out.println("Thread [" + t + "] has not finished.");
          alive = true;
        }
      }
    }

    for (AtomicReference<Throwable> t : errors) {
      Throwable throwable = t.get();
      assertNull(throwable == null ? null : throwable.getMessage(), throwable);
    }

    long oldest = -1;
    long newest = -1;
    for (AtomicLong time : timeRegisteredLst) {
      long l = time.get();
      if (oldest == -1 || l < oldest) {
        oldest = l;
      }
      if (newest == -1 || l > newest) {
        newest = l;
      }
    }
    assertTrue("newest [" + newest + "] oldest [" + oldest + "]", (newest - oldest) < TimeUnit.SECONDS.toMillis(5));
    ZooKeeperLockManager zooKeeperLockManager = new ZooKeeperLockManager(zk, "/testing/safemode");
    Thread.sleep(5000);
    assertEquals(0, zooKeeperLockManager.getNumberOfLockNodesPresent(SafeMode.STARTUP));
  }

  @Test
  public void testBasicStartupWithMinimum() throws IOException, InterruptedException, KeeperException {
    List<AtomicReference<Throwable>> errors = new ArrayList<AtomicReference<Throwable>>();
    List<AtomicLong> timeRegisteredLst = new ArrayList<AtomicLong>();
    List<Thread> threads = new ArrayList<Thread>();
    int nodesThatStart = 10;
    int minimumNodeBeforeStarting = 16;
    for (int i = 0; i < 32; i++) {
      AtomicReference<Throwable> ref = new AtomicReference<Throwable>();
      AtomicLong timeRegistered = new AtomicLong();
      errors.add(ref);
      timeRegisteredLst.add(timeRegistered);
      if (i < nodesThatStart) {
        threads.add(startThread(zk, "node" + i, ref, timeRegistered, 0L, minimumNodeBeforeStarting));
      } else {
        threads.add(startThread(zk, "node" + i, ref, timeRegistered, 10000L, minimumNodeBeforeStarting));
      }
      Thread.sleep(100);
    }

    boolean alive = true;
    while (alive) {
      alive = false;
      for (Thread t : threads) {
        t.join(1000);
        if (t.isAlive()) {
          System.out.println("Thread [" + t + "] has not finished.");
          alive = true;
        }
      }
    }

    for (AtomicReference<Throwable> t : errors) {
      Throwable throwable = t.get();
      assertNull(throwable == null ? null : throwable.getMessage(), throwable);
    }

    long oldest = -1;
    long newest = -1;
    for (AtomicLong time : timeRegisteredLst) {
      long l = time.get();
      if (oldest == -1 || l < oldest) {
        oldest = l;
      }
      if (newest == -1 || l > newest) {
        newest = l;
      }
    }
    assertTrue("newest [" + newest + "] oldest [" + oldest + "]", (newest - oldest) < TimeUnit.SECONDS.toMillis(5));
    ZooKeeperLockManager zooKeeperLockManager = new ZooKeeperLockManager(zk, "/testing/safemode");
    Thread.sleep(5000);
    assertEquals(0, zooKeeperLockManager.getNumberOfLockNodesPresent(SafeMode.STARTUP));
  }

  @Test
  public void testExtraNodeStartup() throws IOException, InterruptedException, KeeperException {
    ZooKeeper zk = new ZooKeeperClient(SuiteCluster.getZooKeeperConnStr(SafeModeTestIT.class), 20000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });

    SafeMode setupSafeMode = new SafeMode(zk, "/testing/safemode", "/testing/nodepath", TimeUnit.SECONDS, 5,
        TimeUnit.SECONDS, 60, 0);
    setupSafeMode.registerNode("node1", null);

    SafeMode safeMode = new SafeMode(zk, "/testing/safemode", "/testing/nodepath", TimeUnit.SECONDS, 5,
        TimeUnit.SECONDS, 60, 0);
    long s = System.nanoTime();
    safeMode.registerNode("node101", null);
    long e = System.nanoTime();

    assertTrue((e - s) < TimeUnit.SECONDS.toNanos(1));
    zk.close();
  }

  @Test
  public void testSecondNodeStartup() throws IOException, InterruptedException, KeeperException {
    ZooKeeper zk = new ZooKeeperClient(SuiteCluster.getZooKeeperConnStr(SafeModeTestIT.class), 20000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });

    SafeMode setupSafeMode = new SafeMode(zk, "/testing/safemode", "/testing/nodepath", TimeUnit.SECONDS, 5,
        TimeUnit.SECONDS, 60, 0);
    setupSafeMode.registerNode("node10", null);

    SafeMode safeMode = new SafeMode(zk, "/testing/safemode", "/testing/nodepath", TimeUnit.SECONDS, 5,
        TimeUnit.SECONDS, 15, 0);
    try {
      safeMode.registerNode("node10", null);
      fail("should throw exception.");
    } catch (Exception e) {
    }
    zk.close();
  }

  private Thread startThread(final ZooKeeper zk, final String node, final AtomicReference<Throwable> errorRef,
      final AtomicLong timeRegistered) {
    return startThread(zk, node, errorRef, timeRegistered, 0L, 0);
  }

  private Thread startThread(final ZooKeeper zk, final String node, final AtomicReference<Throwable> errorRef,
      final AtomicLong timeRegistered, final long waitTime, final int minimumNumberOfNodes) {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(waitTime);
          SafeMode safeMode = new SafeMode(zk, "/testing/safemode", "/testing/nodepath", TimeUnit.SECONDS, 5,
              TimeUnit.SECONDS, 60, minimumNumberOfNodes);
          safeMode.registerNode(node, null);
          timeRegistered.set(System.currentTimeMillis());
        } catch (Throwable t) {
          errorRef.set(t);
        }
      }
    };
    Thread thread = new Thread(runnable);
    thread.start();
    return thread;
  }

  private static void rm(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
    List<String> children = zk.getChildren(path, false);
    for (String c : children) {
      rm(zk, path + "/" + c);
    }
    zk.delete(path, -1);
  }
}
