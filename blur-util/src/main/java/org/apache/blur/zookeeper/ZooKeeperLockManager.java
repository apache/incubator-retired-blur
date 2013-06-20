package org.apache.blur.zookeeper;

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

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperLockManager {

  private static final Log LOG = LogFactory.getLog(ZooKeeperLockManager.class);

  protected final Map<String, String> lockMap = new HashMap<String, String>();
  protected final String lockPath;
  protected final ZooKeeper zooKeeper;
  protected final Object lock = new Object();
  protected final Watcher watcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      synchronized (lock) {
        lock.notify();
      }
    }
  };

  public ZooKeeperLockManager(ZooKeeper zooKeeper, String lockPath) {
    this.zooKeeper = zooKeeper;
    this.lockPath = lockPath;
  }

  public int getNumberOfLockNodesPresent(String name) throws KeeperException, InterruptedException {
    List<String> children = zooKeeper.getChildren(lockPath, false);
    int count = 0;
    for (String s : children) {
      if (s.startsWith(name + "_")) {
        count++;
      }
    }
    return count;
  }

  public void unlock(String name) throws InterruptedException, KeeperException {
    if (!lockMap.containsKey(name)) {
      throw new RuntimeException("Lock [" + name + "] has not be created.");
    }
    String lockPath = lockMap.get(name);
    LOG.debug("Unlocking on path [" + lockPath + "] with name [" + name + "]");
    zooKeeper.delete(lockPath, -1);
  }

  public void lock(String name) throws KeeperException, InterruptedException {
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
