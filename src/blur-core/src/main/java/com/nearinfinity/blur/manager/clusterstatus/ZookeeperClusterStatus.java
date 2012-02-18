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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;

public class ZookeeperClusterStatus extends ClusterStatus {

  private static final Log LOG = LogFactory.getLog(ZookeeperClusterStatus.class);
  private static final Collection<String> EMPTY_COLLECTION = new ArrayList<String>();

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    ZooKeeper zooKeeper = new ZooKeeper("localhost", 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });

    zooKeeper.getChildren("/", false);

    boolean useCache = false;

    ZookeeperClusterStatus status = new ZookeeperClusterStatus(zooKeeper);
    for (int i = 0; i < 1; i++) {
      long s1 = System.nanoTime();
      System.out.println(status.getClusterList());
      long s2 = System.nanoTime();
      System.out.println(status.getControllerServerList());
      long s3 = System.nanoTime();
      System.out.println(status.getOnlineShardServers("default"));
      long s4 = System.nanoTime();
      System.out.println(status.getShardServerList("default"));
      long s5 = System.nanoTime();
      System.out.println(status.getTableList());
      long s6 = System.nanoTime();

      for (String cluster : status.getClusterList()) {
        System.out.println("cluster=" + cluster + " " + status.getOnlineShardServers(cluster));
        System.out.println("cluster=" + cluster + " " + status.getShardServerList(cluster));
      }
      long s7 = System.nanoTime();

      for (String table : status.getTableList()) {
        System.out.println("table=" + table + " " + status.getTableDescriptor(useCache, "default", table));
        System.out.println(status.exists(useCache, "default", table));
        System.out.println(status.isEnabled(useCache, "default", table));
      }
      long s8 = System.nanoTime();

      System.out.println(s2 - s1);
      System.out.println(s3 - s2);
      System.out.println(s4 - s3);
      System.out.println(s5 - s4);
      System.out.println(s6 - s5);
      System.out.println(s7 - s6);
      System.out.println(s8 - s7);
    }
  }

  private ZooKeeper _zk;
  private AtomicBoolean _running = new AtomicBoolean();
  private ConcurrentMap<String, AtomicBoolean> _enabledMap = new ConcurrentHashMap<String, AtomicBoolean>();
  private ConcurrentMap<String, AtomicBoolean> _readOnlyMap = new ConcurrentHashMap<String, AtomicBoolean>();
  private ConcurrentMap<String, Collection<String>> _fieldsMap = new ConcurrentHashMap<String, Collection<String>>();
  private AtomicReference<Map<String, List<String>>> _tableToClusterCache = new AtomicReference<Map<String, List<String>>>(new HashMap<String, List<String>>());
  private Thread _enabledTables;
  private Thread _tablesToCluster;

  public ZookeeperClusterStatus(ZooKeeper zooKeeper) {
    _zk = zooKeeper;
    _running.set(true);
    watchForEnabledTables();
    watchForTables();
  }

  public ZookeeperClusterStatus(String connectionStr) throws IOException {
    this(new ZooKeeper(connectionStr, 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    }));
  }

  private void watchForTables() {
    _tablesToCluster = new Thread(new Runnable() {
      private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          synchronized (_tableToClusterCache) {
            _tableToClusterCache.notifyAll();
          }
        }
      };
      
      @Override
      public void run() {
        while (_running.get()) {
          try {
            doWatch();
          } catch (Throwable t) {
            LOG.error("unknown error", t);
          }
        }
      }

      private void doWatch() throws KeeperException, InterruptedException {
        synchronized (_tableToClusterCache) {
          String clusterPath = ZookeeperPathConstants.getClustersPath();
          List<String> clusters = _zk.getChildren(forPathToExist(clusterPath), watcher);
          Map<String, List<String>> newValue = new HashMap<String, List<String>>();
          for (String cluster : clusters) {
            List<String> tables = _zk.getChildren(forPathToExist(ZookeeperPathConstants.getTablesPath(cluster)), watcher);
            for (String table : tables) {
              List<String> clusterList = newValue.get(table);
              if (clusterList == null) {
                clusterList = new ArrayList<String>();
                newValue.put(table, clusterList);
              }
              clusterList.add(cluster);
            }
          }
          _tableToClusterCache.set(newValue);
          _tableToClusterCache.wait(BlurConstants.ZK_WAIT_TIME);
        }
      }
    });
    _tablesToCluster.setDaemon(true);
    _tablesToCluster.setName("cluster-status-tables-to-cluster-watcher");
    _tablesToCluster.start();
  }

  private void watchForEnabledTables() {
    _enabledTables = new Thread(new Runnable() {
      private Watcher _watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          synchronized (_enabledMap) {
            _enabledMap.notifyAll();
          }
        }
      };
      
      @Override
      public void run() {
        while (_running.get()) {
          try {
            doWatch();
          } catch (Throwable t) {
            LOG.error("unknown error", t);
          }
        }
      }

      private void doWatch() throws KeeperException, InterruptedException {
        synchronized (_enabledMap) {
          String clusterPath = ZookeeperPathConstants.getClustersPath();
          List<String> clusters = _zk.getChildren(forPathToExist(clusterPath), _watcher);
          for (String cluster : clusters) {
            List<String> tables = _zk.getChildren(forPathToExist(ZookeeperPathConstants.getTablesPath(cluster)), _watcher);
            for (String table : tables) {
              Stat stat = _zk.exists(ZookeeperPathConstants.getTableEnabledPath(cluster, table), _watcher);
              AtomicBoolean enabled = _enabledMap.get(table);
              if (enabled == null) {
                enabled = new AtomicBoolean();
                _enabledMap.put(getClusterTableKey(cluster, table), enabled);
              }
              if (stat == null) {
                enabled.set(false);
              } else {
                enabled.set(true);
              }
            }
          }
          _enabledMap.wait(BlurConstants.ZK_WAIT_TIME);
        }
      }
    });
    _enabledTables.setDaemon(true);
    _enabledTables.setName("cluster-status-enabled-tables-watcher");
    _enabledTables.start();
  }
  
  private String forPathToExist(String path) throws KeeperException, InterruptedException {
    Stat stat = _zk.exists(path, false);
    while (stat == null) {
      LOG.info("Waiting for path [{0}] to exist before continuing.",path);
      Thread.sleep(1000);
      stat = _zk.exists(path, false);
    }
    return path;
  }

  private String getClusterTableKey(String cluster, String table) {
    return cluster + "." + table;
  }

  @Override
  public List<String> getClusterList() {
    try {
      return _zk.getChildren(ZookeeperPathConstants.getClustersPath(), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> getControllerServerList() {
    try {
      return _zk.getChildren(ZookeeperPathConstants.getOnlineControllersPath(), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> getOnlineShardServers(String cluster) {
    try {
      return _zk.getChildren(ZookeeperPathConstants.getClustersPath() + "/" + cluster + "/online/shard-nodes", false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> getShardServerList(String cluster) {
    try {
      return _zk.getChildren(ZookeeperPathConstants.getClustersPath() + "/" + cluster + "/shard-nodes", false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean exists(boolean useCache, String cluster, String table) {
    if (useCache) {
      AtomicBoolean enabled = _enabledMap.get(getClusterTableKey(cluster, table));
      if (enabled == null) {
        return false;
      } else {
        return true;
      }
    }
    try {
      if (_zk.exists(ZookeeperPathConstants.getTablePath(cluster, table), false) == null) {
        return false;
      }
      return true;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isEnabled(boolean useCache, String cluster, String table) {
    if (useCache) {
      AtomicBoolean enabled = _enabledMap.get(getClusterTableKey(cluster, table));
      if (enabled == null) {
        throw new RuntimeException("Table [" + table + "] does not exist.");
      } else {
        return enabled.get();
      }
    }
    String tablePathIsEnabled = ZookeeperPathConstants.getTableEnabledPath(cluster, table);
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
  public TableDescriptor getTableDescriptor(boolean useCache, String cluster, String table) {
    TableDescriptor tableDescriptor = new TableDescriptor();
    try {
      if (_zk.exists(ZookeeperPathConstants.getTableEnabledPath(cluster, table), false) == null) {
        tableDescriptor.isEnabled = false;
      } else {
        tableDescriptor.isEnabled = true;
      }
      tableDescriptor.shardCount = Integer.parseInt(new String(getData(ZookeeperPathConstants.getTableShardCountPath(cluster, table))));
      tableDescriptor.tableUri = new String(getData(ZookeeperPathConstants.getTableUriPath(cluster, table)));
      tableDescriptor.compressionClass = new String(getData(ZookeeperPathConstants.getTableCompressionCodecPath(cluster, table)));
      tableDescriptor.compressionBlockSize = Integer.parseInt(new String(getData(ZookeeperPathConstants.getTableCompressionBlockSizePath(cluster, table))));
      tableDescriptor.analyzerDefinition = getAnalyzerDefinition(getData(ZookeeperPathConstants.getTablePath(cluster, table)));
      tableDescriptor.blockCaching = isBlockCacheEnabled(cluster, table);
      tableDescriptor.blockCachingFileTypes = getBlockCacheFileTypes(cluster, table);
      tableDescriptor.name = table;
      byte[] data = getData(ZookeeperPathConstants.getTableSimilarityPath(cluster, table));
      if (data != null) {
        tableDescriptor.similarityClass = new String(data);
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    tableDescriptor.cluster = cluster;
    return tableDescriptor;
  }

  // private int getShardCountFromTablePath(String path) throws
  // NumberFormatException, KeeperException, InterruptedException {
  // return Integer.parseInt(new String(getData(path)));
  // }

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
  public List<String> getTableList(String cluster) {
    try {
      return _zk.getChildren(ZookeeperPathConstants.getTablesPath(cluster), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    _running.set(false);
  }

  @Override
  public String getCluster(boolean useCache, String table) {
    if (useCache) {
      Map<String, List<String>> map = _tableToClusterCache.get();
      List<String> clusters = map.get(table);
      if (clusters == null || clusters.size() == 0) {
        return null;
      } else if (clusters.size() == 1) {
        return clusters.get(0);
      } else {
        throw new RuntimeException("Table [" + table + "] is registered in more than 1 cluster [" + clusters + "].");
      }
    }
    List<String> clusterList = getClusterList();
    for (String cluster : clusterList) {
      try {
        Stat stat = _zk.exists(ZookeeperPathConstants.getTablePath(cluster, table), false);
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
  public void clearLocks(String cluster, String table) {
    String lockPath = ZookeeperPathConstants.getLockPath(cluster, table);
    try {
      if (_zk.exists(lockPath, false) == null) {
        return;
      }
      List<String> children = _zk.getChildren(lockPath, false);
      for (String c : children) {
        LOG.warn("Removing lock [{0}] for table [{1}]", c, table);
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
      String blurSafemodePath = ZookeeperPathConstants.getSafemodePath(cluster);
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
  public int getShardCount(String cluster, String table) {
    try {
      return Integer.parseInt(new String(getData(ZookeeperPathConstants.getTableShardCountPath(cluster, table))));
    } catch (NumberFormatException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Set<String> getBlockCacheFileTypes(String cluster, String table) {
    try {
      byte[] data = getData(ZookeeperPathConstants.getTableBlockCachingFileTypesPath(cluster, table));
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
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isBlockCacheEnabled(String cluster, String table) {
    try {
      if (_zk.exists(ZookeeperPathConstants.getTableBlockCachingFileTypesPath(cluster, table), false) == null) {
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
  public Collection<String> readCacheFieldsForTable(String cluster, String table) {
    Collection<String> fields = _fieldsMap.get(getClusterTableKey(cluster, table));
    if (fields == null) {
      return EMPTY_COLLECTION;
    }
    return fields;
  }

  @Override
  public void writeCacheFieldsForTable(String cluster, String table, Collection<String> fieldNames) {
    Collection<String> cachedFields = readCacheFieldsForTable(cluster, table);
    for (String fieldName : fieldNames) {
      if (cachedFields.contains(fieldName)) {
        continue;
      }
      String path = ZookeeperPathConstants.getTableFieldNamesPath(cluster, table, fieldName);
      try {
        if (_zk.exists(path, false) == null) {
          try {
            _zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          } catch (KeeperException e) {
            if (e.code() == Code.NODEEXISTS) {
              continue;
            }
            LOG.error("Unknown error", e);
          } catch (InterruptedException e) {
            LOG.error("Unknown error", e);
          }
        }
      } catch (KeeperException e) {
        LOG.error("Unknown error", e);
      } catch (InterruptedException e) {
        LOG.error("Unknown error", e);
      }
    }
  }

  @Override
  public boolean isReadOnly(boolean useCache, String cluster, String table) {
    String key = getClusterTableKey(cluster, table);
    if (useCache) {
      AtomicBoolean flag = _readOnlyMap.get(key);
      if (flag != null) {
        return flag.get();
      }
    }
    String path = ZookeeperPathConstants.getTableReadOnlyPath(cluster, table);
    AtomicBoolean flag = new AtomicBoolean();
    try {
      if (_zk.exists(path, false) == null) {
        flag.set(false);
        return false;
      }
      flag.set(true);
      return true;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      _readOnlyMap.put(key, flag);
    }
  }
}
