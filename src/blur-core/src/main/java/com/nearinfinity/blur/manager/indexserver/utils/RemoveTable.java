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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.clusterstatus.ZookeeperPathConstants;

public class RemoveTable {

  private final static Log LOG = LogFactory.getLog(RemoveTable.class);

  public static void removeTable(ZooKeeper zookeeper, String cluster, String table, boolean deleteIndexFiles) throws IOException, KeeperException, InterruptedException {
    String blurTablePath = ZookeeperPathConstants.getTablePath(cluster, table);
    if (zookeeper.exists(blurTablePath, false) == null) {
      throw new IOException("Table [" + table + "] does not exist.");
    }
    if (zookeeper.exists(ZookeeperPathConstants.getTableEnabledPath(cluster, table), false) != null) {
      throw new IOException("Table [" + table + "] must be disabled before it can be removed.");
    }
    String uri = getUri(zookeeper, cluster, table);
    removeAll(zookeeper, blurTablePath);
    if (deleteIndexFiles) {
      removeIndexFiles(uri);
    }
  }

  private static String getUri(ZooKeeper zookeeper, String cluster, String table) throws KeeperException, InterruptedException {
    String path = ZookeeperPathConstants.getTableUriPath(cluster, table);
    Stat stat = zookeeper.exists(path, false);
    if (stat == null) {
      throw new RuntimeException("Uri missing for table [" + table + "]");
    }
    byte[] data = zookeeper.getData(path, false, stat);
    if (data == null) {
      throw new RuntimeException("Uri missing for table [" + table + "]");
    }
    return new String(data);
  }

  private static void removeAll(ZooKeeper zookeeper, String path) throws KeeperException, InterruptedException {
    List<String> list = zookeeper.getChildren(path, false);
    for (String p : list) {
      removeAll(zookeeper, path + "/" + p);
    }
    LOG.info("Removing path [{0}]", path);
    zookeeper.delete(path, -1);
  }

  private static void removeIndexFiles(String uri) throws IOException {
    Path tablePath = new Path(uri);
    FileSystem fileSystem = FileSystem.get(tablePath.toUri(), new Configuration());
    fileSystem.delete(tablePath, true);
  }

}
