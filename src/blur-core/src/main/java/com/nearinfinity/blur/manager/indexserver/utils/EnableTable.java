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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.manager.clusterstatus.ZookeeperPathConstants;

public class EnableTable {

  public static void enableTable(ZooKeeper zookeeper, String cluster, String table) throws IOException, KeeperException, InterruptedException {
    if (zookeeper.exists(ZookeeperPathConstants.getTablePath(cluster, table) , false) == null) {
      throw new IOException("Table [" + table + "] does not exist.");
    }
    String blurTableEnabledPath = ZookeeperPathConstants.getTableEnabledPath(cluster, table);
    if (zookeeper.exists(blurTableEnabledPath, false) != null) {
      throw new IOException("Table [" + table + "] already enabled.");
    }
    zookeeper.create(blurTableEnabledPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

}
