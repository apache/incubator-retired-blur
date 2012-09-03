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
import java.io.IOException;
import java.util.List;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class ZkUtils {

  private final static Log LOG = LogFactory.getLog(ZkUtils.class);

  public static final int ANY_VERSION = -1;

  public static final int DEFAULT_ZK_SESSION_TIMEOUT = 60000;

  public static class ConnectionWatcher implements Watcher {

    private String zkConnectionString;
    private int sessionTimeout;

    public void setZkConnectionString(String zkConnectionString) {
      this.zkConnectionString = zkConnectionString;
    }

    public void setSessionTimeout(int sessionTimeout) {
      this.sessionTimeout = sessionTimeout;
    }

    @Override
    public void process(WatchedEvent event) {
      KeeperState state = event.getState();
      LOG.info("ZooKeeper [{0}] timeout [{1}] changed to [{2}] state", zkConnectionString, sessionTimeout, state);
    }

  }

  public static ZooKeeper newZooKeeper(String zkConnectionString) throws IOException {
    int sessionTimeout = DEFAULT_ZK_SESSION_TIMEOUT;
    ConnectionWatcher watcher = new ConnectionWatcher();
    watcher.setSessionTimeout(sessionTimeout);
    watcher.setZkConnectionString(zkConnectionString);
    return new ZooKeeper(zkConnectionString, sessionTimeout, watcher);
  }

  public static void mkNodesStr(ZooKeeper zk, String path) {
    if (path == null) {
      return;
    }
    mkNodes(zk, path.split("/"));
  }

  public static void mkNodes(ZooKeeper zk, String... path) {
    if (path == null) {
      return;
    }
    for (int i = 0; i < path.length; i++) {
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j <= i; j++) {
        if (!path[j].isEmpty()) {
          builder.append('/');
          builder.append(path[j]);
        }
      }
      String pathToCheck = removeDupSeps(builder.toString());
      if (pathToCheck.isEmpty()) {
        continue;
      }
      try {
        if (zk.exists(pathToCheck, false) == null) {
          zk.create(pathToCheck, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
      } catch (NodeExistsException e) {
        // do nothing
      } catch (KeeperException e) {
        LOG.error("error", e);
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        LOG.error("error", e);
        throw new RuntimeException(e);
      }
    }
  }

  private static String removeDupSeps(String path) {
    return path.replace("//", "/");
  }

  public static String getPath(String... parts) {
    if (parts == null || parts.length == 0) {
      return null;
    }
    StringBuilder builder = new StringBuilder(parts[0]);
    for (int i = 1; i < parts.length; i++) {
      builder.append('/');
      builder.append(parts[i]);
    }
    return builder.toString();
  }

  public static boolean exists(ZooKeeper zk, String... path) {
    if (path == null || path.length == 0) {
      return true;
    }
    StringBuilder builder = new StringBuilder(path[0]);
    for (int i = 1; i < path.length; i++) {
      builder.append('/').append(path[i]);
    }
    try {
      return zk.exists(builder.toString(), false) != null;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void deleteAnyVersion(ZooKeeper zk, String path) {
    try {
      List<String> children = zk.getChildren(path, false);
      for (String c : children) {
        deleteAnyVersion(zk, path + "/" + c);
      }
      zk.delete(path, ANY_VERSION);
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.NONODE) {
        return;
      }
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void waitUntilExists(ZooKeeper zooKeeper, String path) {
    final Object o = new Object();
    try {
      while (true) {
        Stat stat = zooKeeper.exists(path, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            synchronized (o) {
              o.notifyAll();
            }
          }
        });
        if (stat == null) {
          synchronized (o) {
            o.wait();
          }
        } else {
          return;
        }
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
