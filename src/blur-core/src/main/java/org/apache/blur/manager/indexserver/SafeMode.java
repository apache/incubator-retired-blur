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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * This class controls the startup of the cluster. Basically the first node
 * online waits until there is no more nodes that have started. The period that
 * is required to have no activity is the waittime passed in through the
 * constructor. If a new node comes online while the leader is waiting, the wait
 * starts over. Once the wait period has been exhausted the cluster is to be
 * settled and can now come online.
 * 
 */
public class SafeMode {

  private static final Log LOG = LogFactory.getLog(SafeMode.class);
  private static final String STARTUP = "STARTUP";
  private static final String SETUP = "SETUP";

  private final ZooKeeper zooKeeper;
  private final String lockPath;
  private final long waitTime;
  private final Object lock = new Object();
  private final Watcher watcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      synchronized (lock) {
        lock.notify();
      }
    }
  };
  private final Map<String, String> lockMap = new HashMap<String, String>();
  private final String nodePath;
  private final long duplicateNodeTimeout;

  public SafeMode(ZooKeeper zooKeeper, String lockPath, String nodePath, TimeUnit waitTimeUnit, long waitTime,
      TimeUnit duplicateNodeTimeoutTimeUnit, long duplicateNodeTimeout) {
    this.zooKeeper = zooKeeper;
    this.lockPath = lockPath;
    this.waitTime = waitTimeUnit.toMillis(waitTime);
    this.duplicateNodeTimeout = duplicateNodeTimeoutTimeUnit.toNanos(duplicateNodeTimeout);
    this.nodePath = nodePath;
  }

  public void registerNode(String node, byte[] data) throws KeeperException, InterruptedException {
    lock(SETUP);
    register(node, data);
    if (isLeader(node)) {
      // Create barrier for cluster
      lock(STARTUP);

      // Allow other nodes to register
      unlock(SETUP);
      waitForClusterToSettle();
      unlock(STARTUP);
    } else {
      // Allow other nodes to register
      unlock(SETUP);

      // Block waiting on cluster to settle
      lock(STARTUP);
      unlock(STARTUP);
    }
  }

  private void waitForClusterToSettle() throws InterruptedException, KeeperException {
    long startingWaitTime = System.currentTimeMillis();
    List<String> prev = null;
    while (true) {
      synchronized (lock) {
        List<String> children = new ArrayList<String>(zooKeeper.getChildren(nodePath, watcher));
        Collections.sort(children);
        if (children.equals(prev)) {
          LOG.info("Clustered has settled.");
          return;
        } else {
          prev = children;
          LOG.info("Waiting for cluster to settle, current size [" + children.size() + "] total time waited so far ["
              + (System.currentTimeMillis() - startingWaitTime) + " ms] waiting another [" + waitTime + " ms].");
          lock.wait(waitTime);
        }
      }
    }
  }

  private boolean isLeader(String node) throws KeeperException, InterruptedException {
    List<String> children = zooKeeper.getChildren(nodePath, false);
    if (children.size() == 1) {
      String n = children.get(0);
      if (!n.equals(node)) {
        throw new RuntimeException("We got a problem here!  Only one node register [" + n + "] and I'm not it [" + node
            + "]");
      }
      return true;
    }
    return false;
  }

  private void unlock(String name) throws InterruptedException, KeeperException {
    if (!lockMap.containsKey(name)) {
      throw new RuntimeException("Lock [" + name + "] has not be created.");
    }
    String lockPath = lockMap.get(name);
    LOG.debug("Unlocking on path [" + lockPath + "] with name [" + name + "]");
    zooKeeper.delete(lockPath, -1);
  }

  private void register(String node, byte[] data) throws KeeperException, InterruptedException {
    String p = nodePath + "/" + node;
    long start = System.nanoTime();
    while (zooKeeper.exists(p, false) != null) {
      if (start + duplicateNodeTimeout < System.nanoTime()) {
        throw new RuntimeException("Node [" + node + "] cannot be registered, check to make sure a "
            + "process has not already been started or that server" + " names have not been duplicated.");
      }
      LOG.info("Node [{0}] already registered, waiting for path [{1}] to be released", node, p);
      String tmpPath = p + "_" + UUID.randomUUID();
      zooKeeper.create(tmpPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      Thread.sleep(1000);
      zooKeeper.delete(tmpPath, -1);
    }
    zooKeeper.create(p, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
  }

  private void lock(String name) throws KeeperException, InterruptedException {
    if (lockMap.containsKey(name)) {
      throw new RuntimeException("Lock [" + name + "] already created.");
    }
    String newPath = zooKeeper.create(lockPath + "/" + name + "_", null, Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL);
    lockMap.put(name, newPath);
    while (true) {
      synchronized (lock) {
        List<String> children = getOnlyThisLocksChildren(name, zooKeeper.getChildren(lockPath, watcher));
        Collections.sort(children);
        String firstElement = children.get(0);
        if ((lockPath + "/" + firstElement).equals(newPath)) {
          // yay!, we got the lock
          LOG.debug("Lock on path [" + lockPath + "] with name [" + name + "]");
          return;
        } else {
          LOG.debug("Waiting for lock on path [" + lockPath + "] with name [" + name + "]");
          lock.wait();
        }
      }
    }
  }

  private List<String> getOnlyThisLocksChildren(String name, List<String> children) {
    List<String> result = new ArrayList<String>();
    for (String c : children) {
      if (c.startsWith(name + "_")) {
        result.add(c);
      }
    }
    return result;
  }

}
