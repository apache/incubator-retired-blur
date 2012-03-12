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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.manager.clusterstatus.ZookeeperPathConstants;

public class DisableTable {

//  private final static Log LOG = LogFactory.getLog(DisableTable.class);

  public static void disableTable(ZooKeeper zookeeper, String cluster, String table) throws IOException, InterruptedException, KeeperException {
    if (zookeeper.exists(ZookeeperPathConstants.getTablePath(cluster, table) , false) == null) {
      throw new IOException("Table [" + table + "] does not exist.");
    }
    String blurTableEnabledPath = ZookeeperPathConstants.getTableEnabledPath(cluster, table);
    if (zookeeper.exists(blurTableEnabledPath , false) == null) {
      throw new IOException("Table [" + table + "] already disabled.");
    }
    zookeeper.delete(blurTableEnabledPath, -1);
//    waitForWriteLocksToClear(zookeeper, cluster, table);
  }

//  private static void waitForWriteLocksToClear(ZooKeeper zookeeper, String cluster, String table) throws KeeperException, InterruptedException {
//    final Object object = new Object();
//    String path = ZookeeperPathConstants.getLockPath(cluster, table);
//    while (true) {
//      synchronized (object) {
//        List<String> list = zookeeper.getChildren(path, new Watcher() {
//          @Override
//          public void process(WatchedEvent event) {
//            synchronized (object) {
//              object.notifyAll();
//            }
//          }
//        });
//        if (list.isEmpty()) {
//          LOG.info("All [{0}] locks for table [{1}]", list.size(), table);
//          return;
//        } else {
//          LOG.info("Waiting for locks to be released [{0}] total [{1}]", list.size(), list);
//          object.wait(BlurConstants.ZK_WAIT_TIME);
//        }
//      }
//    }
//  }
}
