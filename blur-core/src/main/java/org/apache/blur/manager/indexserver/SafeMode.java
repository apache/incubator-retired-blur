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

  private final ZooKeeper _zooKeeper;
  private final long _waitTime;
  private final String _nodePath;
  private final long _duplicateNodeTimeout;
  private final int _minimumNumberOfNodes;

  public SafeMode(ZooKeeper zooKeeper, String lockPath, String nodePath, TimeUnit waitTimeUnit, long waitTime,
      TimeUnit duplicateNodeTimeoutTimeUnit, long duplicateNodeTimeout, int minimumNumberOfNodes) {
    super(zooKeeper, lockPath);
    _zooKeeper = zooKeeper;
    _waitTime = waitTimeUnit.toMillis(waitTime);
    _duplicateNodeTimeout = duplicateNodeTimeoutTimeUnit.toNanos(duplicateNodeTimeout);
    _nodePath = nodePath;
    _minimumNumberOfNodes = minimumNumberOfNodes;
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
      synchronized (_lock) {
        List<String> children = new ArrayList<String>(_zooKeeper.getChildren(_nodePath, _watcher));
        Collections.sort(children);
        if (children.equals(prev) && children.size() >= _minimumNumberOfNodes) {
          LOG.info("Cluster has settled.");
          return;
        } else {
          prev = children;
          LOG.info(
              "Waiting for cluster to settle, current size [{0}] min [{1}] total time waited so far [{2} ms] waiting another [{3} ms].",
              children.size(), _minimumNumberOfNodes, (System.currentTimeMillis() - startingWaitTime), _waitTime);
          _lock.wait(_waitTime);
        }
      }
    }
  }

  private boolean isLeader(String node) throws KeeperException, InterruptedException {
    List<String> children = _zooKeeper.getChildren(_nodePath, false);
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
    String p = _nodePath + "/" + node;
    long start = System.nanoTime();
    while (_zooKeeper.exists(p, false) != null) {
      if (start + _duplicateNodeTimeout < System.nanoTime()) {
        throw new RuntimeException("Node [" + node + "] cannot be registered, check to make sure a "
            + "process has not already been started or that server" + " names have not been duplicated.");
      }
      LOG.info("Node [{0}] already registered, waiting for path [{1}] to be released", node, p);
      String tmpPath = p + "_" + UUID.randomUUID();
      _zooKeeper.create(tmpPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      Thread.sleep(1000);
      _zooKeeper.delete(tmpPath, -1);
    }
    _zooKeeper.create(p, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
  }

}
