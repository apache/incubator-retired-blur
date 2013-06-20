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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.blur.zookeeper.ZooKeeperLockManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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
public class SafeMode extends ZooKeeperLockManager {

  private static final Log LOG = LogFactory.getLog(SafeMode.class);
  public static final String STARTUP = "STARTUP";
  public static final String SETUP = "SETUP";

  private final ZooKeeper zooKeeper;
  
  private final long waitTime;
  
  private final String nodePath;
  private final long duplicateNodeTimeout;

  public SafeMode(ZooKeeper zooKeeper, String lockPath, String nodePath, TimeUnit waitTimeUnit, long waitTime,
      TimeUnit duplicateNodeTimeoutTimeUnit, long duplicateNodeTimeout) {
    super(zooKeeper,lockPath);
    this.zooKeeper = zooKeeper;
    this.waitTime = waitTimeUnit.toMillis(waitTime);
    this.duplicateNodeTimeout = duplicateNodeTimeoutTimeUnit.toNanos(duplicateNodeTimeout);
    this.nodePath = nodePath;
    ZkUtils.mkNodesStr(zooKeeper, nodePath);
    ZkUtils.mkNodesStr(zooKeeper, lockPath);
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

}
