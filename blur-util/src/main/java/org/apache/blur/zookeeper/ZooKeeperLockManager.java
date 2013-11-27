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
import java.util.concurrent.TimeUnit;

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

  protected final Map<String, String> _lockMap = new HashMap<String, String>();
  protected final String _lockPath;
  protected final ZooKeeper _zooKeeper;
  protected final Object _lock = new Object();
  protected final long _timeout;
  protected final Watcher _watcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      synchronized (_lock) {
        _lock.notify();
      }
    }
  };

  public ZooKeeperLockManager(ZooKeeper zooKeeper, String lockPath) {
    _zooKeeper = zooKeeper;
    _lockPath = lockPath;
    _timeout = TimeUnit.SECONDS.toMillis(1);
  }

  public int getNumberOfLockNodesPresent(String name) throws KeeperException, InterruptedException {
    List<String> children = _zooKeeper.getChildren(_lockPath, false);
    int count = 0;
    for (String s : children) {
      if (s.startsWith(name + "_")) {
        count++;
      }
    }
    return count;
  }

  public void unlock(String name) throws InterruptedException, KeeperException {
    if (!_lockMap.containsKey(name)) {
      throw new RuntimeException("Lock [" + name + "] has not be created.");
    }
    String lockPath = _lockMap.remove(name);
    LOG.debug("Unlocking on path [" + lockPath + "] with name [" + name + "]");
    _zooKeeper.delete(lockPath, -1);
  }

  public void lock(String name) throws KeeperException, InterruptedException {
    if (_lockMap.containsKey(name)) {
      throw new RuntimeException("Lock [" + name + "] already created.");
    }
    String newPath = _zooKeeper.create(_lockPath + "/" + name + "_", null, Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL);
    _lockMap.put(name, newPath);
    while (true) {
      synchronized (_lock) {
        List<String> children = getOnlyThisLocksChildren(name, _zooKeeper.getChildren(_lockPath, _watcher));
        Collections.sort(children);
        String firstElement = children.get(0);
        if ((_lockPath + "/" + firstElement).equals(newPath)) {
          // yay!, we got the lock
          LOG.debug("Lock on path [" + _lockPath + "] with name [" + name + "]");
          return;
        } else {
          LOG.debug("Waiting for lock on path [" + _lockPath + "] with name [" + name + "]");
          _lock.wait(_timeout);
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
