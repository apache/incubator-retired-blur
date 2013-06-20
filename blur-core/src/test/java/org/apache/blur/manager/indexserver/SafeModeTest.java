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
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.MiniCluster;
import org.apache.blur.zookeeper.ZooKeeperLockManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SafeModeTest {

  private static String path = "./target/test-zk";
  private static ZooKeeper zk;

  @BeforeClass
  public static void startZooKeeper() throws IOException {
    new File(path).mkdirs();
    MiniCluster.startZooKeeper(path);
    zk = new ZooKeeper(MiniCluster.getZkConnectionString(), 20000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
  }

  @AfterClass
  public static void stopZooKeeper() throws InterruptedException {
    zk.close();
    MiniCluster.shutdownZooKeeper();
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

    for (Thread t : threads) {
      t.join();
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
    ZooKeeper zk = new ZooKeeper(MiniCluster.getZkConnectionString(), 20000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });

    SafeMode safeMode = new SafeMode(zk, "/testing/safemode", "/testing/nodepath", TimeUnit.SECONDS, 5,
        TimeUnit.SECONDS, 60);
    long s = System.nanoTime();
    safeMode.registerNode("node101", null);
    long e = System.nanoTime();

    assertTrue((e - s) < TimeUnit.SECONDS.toNanos(1));
    zk.close();
  }

  @Test
  public void testSecondNodeStartup() throws IOException, InterruptedException, KeeperException {
    ZooKeeper zk = new ZooKeeper(MiniCluster.getZkConnectionString(), 20000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });

    SafeMode safeMode = new SafeMode(zk, "/testing/safemode", "/testing/nodepath", TimeUnit.SECONDS, 5,
        TimeUnit.SECONDS, 15);
    try {
      safeMode.registerNode("node10", null);
      fail("should throw exception.");
    } catch (Exception e) {
    }
    zk.close();
  }

  private Thread startThread(final ZooKeeper zk, final String node, final AtomicReference<Throwable> errorRef,
      final AtomicLong timeRegistered) {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          SafeMode safeMode = new SafeMode(zk, "/testing/safemode", "/testing/nodepath", TimeUnit.SECONDS, 5,
              TimeUnit.SECONDS, 60);
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

}
