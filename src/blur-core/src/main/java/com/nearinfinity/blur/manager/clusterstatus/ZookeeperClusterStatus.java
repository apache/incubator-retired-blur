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

package com.nearinfinity.blur.manager.clusterstatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public class ZookeeperClusterStatus extends ClusterStatus {

  private static final Log LOG = LogFactory.getLog(ZookeeperClusterStatus.class);

  private ZooKeeper _zk;

  public static void main(String[] args) throws IOException {
    ZooKeeper zooKeeper = new ZooKeeper("localhost", 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
    ZookeeperClusterStatus status = new ZookeeperClusterStatus(zooKeeper);
    for (int i = 0; i < 1; i++) {
      System.out.println(status.getClusterList());
      System.out.println(status.getControllerServerList());
      System.out.println(status.getOnlineShardServers("default"));
      System.out.println(status.getShardServerList("default"));
      System.out.println(status.getTableList());

      for (String cluster : status.getClusterList()) {
        System.out.println("cluster=" + cluster + " " + status.getOnlineShardServers(cluster));
        System.out.println("cluster=" + cluster + " " + status.getShardServerList(cluster));
      }
      for (String table : status.getTableList()) {
        System.out.println("table=" + table + " " + status.getTableDescriptor(table));
        System.out.println(status.exists(table));
        System.out.println(status.isEnabled(table));
      }
    }
  }

  public ZookeeperClusterStatus(ZooKeeper zooKeeper) {
    _zk = zooKeeper;
  }

  @Override
  public List<String> getClusterList() {
    try {
      return _zk.getChildren(ZookeeperPathConstants.getBlurClusterPath(), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> getControllerServerList() {
    try {
      return _zk.getChildren(ZookeeperPathConstants.getBlurOnlineControllersPath(), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized List<String> getOnlineShardServers(String cluster) {
    try {
      return _zk.getChildren(ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/online/shard-nodes", false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> getShardServerList(String cluster) {
    try {
      return _zk.getChildren(ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/shard-nodes", false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean exists(String table) {
    String cluster = getCluster(table);
    if (cluster == null) {
      return false;
    }
    return true;
  }

  @Override
  public boolean isEnabled(String table) {
    String cluster = getCluster(table);
    if (cluster == null) {
      return false;
    }
    String tablePathIsEnabled = ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/tables/" + table + "/" + ZookeeperPathConstants.getBlurTablesEnabled();
    try {
      if (_zk.exists(tablePathIsEnabled, false) == null) {
        return false;
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  @Override
  public TableDescriptor getTableDescriptor(String table) {
    String cluster = getCluster(table);
    if (cluster == null) {
      return null;
    }
    String tablePath = ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/tables/" + table;
    TableDescriptor tableDescriptor = new TableDescriptor();
    try {
      if (_zk.exists(tablePath + "/enabled", false) == null) {
        tableDescriptor.isEnabled = false;
      } else {
        tableDescriptor.isEnabled = true;
      }
      tableDescriptor.shardCount = getShardCountFromTablePath(tablePath);
      tableDescriptor.tableUri = new String(getData(tablePath + "/uri"));
      tableDescriptor.compressionClass = new String(getData(tablePath + "/compression-codec"));
      tableDescriptor.compressionBlockSize = Integer.parseInt(new String(getData(tablePath + "/compression-blocksize")));
      tableDescriptor.analyzerDefinition = getAnalyzerDefinition(getData(tablePath));
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    tableDescriptor.cluster = cluster;
    return tableDescriptor;
  }

  private int getShardCountFromTablePath(String tablePath) throws NumberFormatException, KeeperException, InterruptedException {
    return Integer.parseInt(new String(getData(tablePath + "/shard-count")));
  }

  private AnalyzerDefinition getAnalyzerDefinition(byte[] data) {
    TMemoryInputTransport trans = new TMemoryInputTransport(data);
    TJSONProtocol protocol = new TJSONProtocol(trans);
    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    try {
      analyzerDefinition.read(protocol);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    trans.close();
    return analyzerDefinition;
  }

  private byte[] getData(String path) throws KeeperException, InterruptedException {
    Stat stat = _zk.exists(path, false);
    if (stat == null) {
      return null;
    }
    return _zk.getData(path, false, stat);
  }

  @Override
  public List<String> getTableList() {
    List<String> result = new ArrayList<String>();
    for (String cluster : getClusterList()) {
      try {
        result.addAll(_zk.getChildren(ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/tables", false));
      } catch (KeeperException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  public void close() {
    //do nothing now
  }


  @Override
  public String getCluster(String table) {
    List<String> clusterList = getClusterList();
    for (String cluster : clusterList) {
      try {
        String path = ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/tables/" + table;
        Stat stat = _zk.exists(path, false);
        if (stat != null) {
          return cluster;
        }
      } catch (KeeperException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  public void clearLocks(String table) {
    String lockPath = ZookeeperPathConstants.getBlurLockPath(table);
    try {
      List<String> children = _zk.getChildren(lockPath, false);
      for (String c : children) {
        LOG.warn("Removing lock [{0}] for table [{1}]",c,table);
        _zk.delete(lockPath + "/" + c, -1);
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isInSafeMode(String cluster) {
    try {
      String blurSafemodePath = ZookeeperPathConstants.getBlurSafemodePath();
      Stat stat = _zk.exists(blurSafemodePath, false);
      if (stat == null) {
        return false;
      }
      byte[] data = _zk.getData(blurSafemodePath, false, stat);
      if (data == null) {
        return false;
      }
      long timestamp = Long.parseLong(new String(data));
      long waitTime = timestamp - System.currentTimeMillis();
      if (waitTime > 0) {
        return true;
      }
      return false;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getShardCount(String table) {
    String cluster = getCluster(table);
    if (cluster == null) {
      throw new RuntimeException("Cluster not found for table [" + table + "]");
    }
    String tablePath = ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/tables/" + table;
    try {
      return getShardCountFromTablePath(tablePath);
    } catch (NumberFormatException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Set<String> getBlockCacheFileTypes(String table) {
    String cluster = getCluster(table);
    if (cluster == null) {
      throw new RuntimeException("Cluster not found for table [" + table + "]");
    }
    String tablePath = ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/tables/" + table;
    try {
      return getBlockCacheFileTypesFromTablePath(tablePath);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Set<String> getBlockCacheFileTypesFromTablePath(String tablePath) throws KeeperException, InterruptedException {
    byte[] data = getData(tablePath + "/" + ZookeeperPathConstants.getBlurTablesBlockCachingFileTypes());
    if (data == null) {
      return null;
    }
    String str = new String(data);
    if (str.isEmpty()) {
      return null;
    }
    Set<String> types = new HashSet<String>(Arrays.asList(str.split(",")));
    if (types.isEmpty()) {
      return null;
    }
    return types;
  }

  @Override
  public boolean isBlockCacheEnabled(String table) {
    String cluster = getCluster(table);
    if (cluster == null) {
      return false;
    }
    String tablePathIsEnabled = ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/tables/" + table + "/" + ZookeeperPathConstants.getBlurTablesBlockCaching();
    try {
      if (_zk.exists(tablePathIsEnabled, false) == null) {
        return false;
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return true;
  }
}
