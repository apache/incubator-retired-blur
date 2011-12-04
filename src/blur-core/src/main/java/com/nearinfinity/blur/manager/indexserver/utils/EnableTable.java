/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.manager.indexserver.utils;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.clusterstatus.ZookeeperPathConstants;

public class EnableTable {

  private final static Log LOG = LogFactory.getLog(EnableTable.class);

  public static void enableTable(ZooKeeper zookeeper, String cluster, String table) throws IOException, KeeperException, InterruptedException {
    if (zookeeper.exists(ZookeeperPathConstants.getTablePath(cluster, table) , false) == null) {
      throw new IOException("Table [" + table + "] does not exist.");
    }
    String blurTableEnabledPath = ZookeeperPathConstants.getTableEnabledPath(cluster, table);
    if (zookeeper.exists(blurTableEnabledPath, false) != null) {
      throw new IOException("Table [" + table + "] already enabled.");
    }
    zookeeper.create(blurTableEnabledPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    int shardCount = getShardCount(zookeeper, cluster, table);
    waitForWriteLocksToEngage(zookeeper, cluster, table, shardCount);
  }

  private static int getShardCount(ZooKeeper zookeeper, String cluster, String table) throws KeeperException, InterruptedException {
    String path = ZookeeperPathConstants.getTableShardCountPath(cluster, table);
    Stat stat = zookeeper.exists(path, false);
    if (stat == null) {
      throw new RuntimeException("Shard count missing for table [" + table + "]");
    }
    byte[] data = zookeeper.getData(path, false, stat);
    if (data == null) {
      throw new RuntimeException("Shard count missing for table [" + table + "]");
    }
    return Integer.parseInt(new String(data));
  }

  private static void waitForWriteLocksToEngage(ZooKeeper zookeeper, String cluster, String table, int shardCount) throws KeeperException, InterruptedException {
    final Object object = new Object();
    String path = ZookeeperPathConstants.getLockPath(cluster, table);
    while (true) {
      synchronized (object) {
        List<String> list = zookeeper.getChildren(path, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            synchronized (object) {
              object.notifyAll();
            }
          }
        });
        if (list.size() == shardCount) {
          LOG.info("All [{0}] locks for table [{1}] has been engaged.",list.size(), table);
          return;
        } else {
          LOG.info("Waiting for locks to engage [{0}] out of [{1}]", list.size(), shardCount);
          object.wait();
        }
      }
    }
  }

}
