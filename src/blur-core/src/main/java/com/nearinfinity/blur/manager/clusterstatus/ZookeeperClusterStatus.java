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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.zookeeper.WatchChildren;
import com.nearinfinity.blur.zookeeper.WatchChildren.OnChange;
import com.nearinfinity.blur.zookeeper.WatchNodeData;
import com.nearinfinity.blur.zookeeper.WatchNodeExistance;

public class ZookeeperClusterStatus extends ClusterStatus {

  private static final Log LOG = LogFactory.getLog(ZookeeperClusterStatus.class);

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    ZooKeeper zooKeeper = new ZooKeeper("localhost", 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });

    zooKeeper.getChildren("/", false);

    boolean useCache = false;

    ZookeeperClusterStatus status = new ZookeeperClusterStatus(zooKeeper);
    // for (int i = 0; i < 1; i++) {
    while (true) {
      System.out.println(status.isInSafeMode(true, "default"));

      List<String> tableList = status.getTableList();
      for (String table : tableList) {
        try {
          String cluster = status.getCluster(true, table);
          boolean enabled = status.isEnabled(true, cluster, table);
          System.out.println("Table [" + table + "] in cluster[" + cluster + "] is enabled [" + enabled + "]");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      Thread.sleep(100);

      // long s1 = System.nanoTime();
      // System.out.println(status.getClusterList());
      // long s2 = System.nanoTime();
      // System.out.println(status.getControllerServerList());
      // long s3 = System.nanoTime();
      // System.out.println(status.getOnlineShardServers(true, "default"));
      // long s4 = System.nanoTime();
      // System.out.println(status.getShardServerList("default"));
      // long s5 = System.nanoTime();
      // System.out.println(status.getTableList());
      // long s6 = System.nanoTime();
      //
      // for (String cluster : status.getClusterList()) {
      // System.out.println("cluster=" + cluster + " " +
      // status.getOnlineShardServers(true, cluster));
      // System.out.println("cluster=" + cluster + " " +
      // status.getShardServerList(cluster));
      // }
      // long s7 = System.nanoTime();
      //
      // for (String table : status.getTableList()) {
      // System.out.println("table=" + table + " " +
      // status.getTableDescriptor(useCache, "default", table));
      // System.out.println(status.exists(useCache, "default", table));
      // System.out.println(status.isEnabled(useCache, "default", table));
      // }
      // long s8 = System.nanoTime();
      //
      // System.out.println(s2 - s1);
      // System.out.println(s3 - s2);
      // System.out.println(s4 - s3);
      // System.out.println(s5 - s4);
      // System.out.println(s6 - s5);
      // System.out.println(s7 - s6);
      // System.out.println(s8 - s7);
    }
  }

  private ZooKeeper _zk;
  private AtomicBoolean _running = new AtomicBoolean();
  private ConcurrentMap<String, Long> _safeModeMap = new ConcurrentHashMap<String, Long>();
  private ConcurrentMap<String, Boolean> _enabledMap = new ConcurrentHashMap<String, Boolean>();
  private ConcurrentMap<String, Boolean> _readOnlyMap = new ConcurrentHashMap<String, Boolean>();
  private AtomicReference<Map<String, String>> _tableToClusterCache = new AtomicReference<Map<String, String>>(new ConcurrentHashMap<String, String>());
  private AtomicReference<Map<String, List<String>>> _onlineShardsNodes = new AtomicReference<Map<String, List<String>>>();
  private WatchChildren clusterWatch;
  private Map<String, WatchChildren> tableWatchers = new ConcurrentHashMap<String, WatchChildren>();
  private Map<String, WatchNodeExistance> enabledTableWatchers = new ConcurrentHashMap<String, WatchNodeExistance>();
  private Map<String, WatchNodeExistance> safeModeWatchers = new ConcurrentHashMap<String, WatchNodeExistance>();
  private Map<String, WatchNodeData> safeModeDataWatchers = new ConcurrentHashMap<String, WatchNodeData>();

  public ZookeeperClusterStatus(ZooKeeper zooKeeper) {
    _zk = zooKeeper;
    _running.set(true);
    watchForClusters();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  class Clusters extends OnChange {
    @Override
    public void action(List<String> clusters) {
      for (String cluster : clusters) {
        if (!tableWatchers.containsKey(cluster)) {
          WatchChildren clusterWatcher = new WatchChildren(_zk, ZookeeperPathConstants.getTablesPath(cluster)).watch(new Tables(cluster));
          tableWatchers.put(cluster, clusterWatcher);
          WatchNodeExistance watchNodeExistance = new WatchNodeExistance(_zk, ZookeeperPathConstants.getSafemodePath(cluster)).watch(new SafeExistance(cluster));
          safeModeWatchers.put(cluster, watchNodeExistance);
        }
      }
      List<String> clustersToCloseAndRemove = new ArrayList<String>(clusters);
      clustersToCloseAndRemove.removeAll(tableWatchers.keySet());
      for (String cluster : clustersToCloseAndRemove) {
        WatchChildren watcher = tableWatchers.remove(cluster);
        if (watcher == null) {
          LOG.error("Error watcher is null [" + cluster + "] ");
        } else {
          watcher.close();
        }
      }
    }
  }

  class SafeExistance extends WatchNodeExistance.OnChange {

    private String cluster;

    public SafeExistance(String cluster) {
      this.cluster = cluster;
    }

    @Override
    public void action(Stat stat) {
      if (stat != null) {
        safeModeDataWatchers.put(cluster, new WatchNodeData(_zk, ZookeeperPathConstants.getSafemodePath(cluster)).watch(new WatchNodeData.OnChange() {
          @Override
          public void action(byte[] data) {
            if (data == null) {
              _safeModeMap.put(cluster, null);
            } else {
              _safeModeMap.put(cluster, Long.parseLong(new String(data)));
            }
          }
        }));
      }
    }
  }

  class Tables extends OnChange {
    private String cluster;

    public Tables(String cluster) {
      this.cluster = cluster;
    }

    @Override
    public void action(List<String> tables) {
      synchronized (_tableToClusterCache) {
        Map<String, String> map = _tableToClusterCache.get();
        for (String t : tables) {
          final String table = t;
          String existingCluster = map.get(table);
          if (existingCluster == null) {
            map.put(table, cluster);
            enabledTableWatchers.put(table, new WatchNodeExistance(_zk, ZookeeperPathConstants.getTableEnabledPath(cluster, table)).watch(new WatchNodeExistance.OnChange() {
              @Override
              public void action(Stat stat) {
                String clusterTableKey = getClusterTableKey(cluster, table);
                if (stat == null) {
                  _enabledMap.put(clusterTableKey, false);
                } else {
                  _enabledMap.put(clusterTableKey, true);
                }
              }
            }));
          } else if (!existingCluster.equals(cluster)) {
            LOG.error("Error table [{0}] is being served by more than one cluster [{1},{2}].", table, existingCluster, cluster);
          }
        }
      }
    }
  }

  private void watchForClusters() {
    clusterWatch = new WatchChildren(_zk, ZookeeperPathConstants.getClustersPath()).watch(new Clusters());
  }

  public ZookeeperClusterStatus(String connectionStr) throws IOException {
    this(new ZooKeeper(connectionStr, 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    }));
  }

  // private String forPathToExist(String path) throws KeeperException,
  // InterruptedException {
  // Stat stat = _zk.exists(path, false);
  // while (stat == null) {
  // LOG.info("Waiting for path [{0}] to exist before continuing.", path);
  // Thread.sleep(1000);
  // stat = _zk.exists(path, false);
  // }
  // return path;
  // }

  private String getClusterTableKey(String cluster, String table) {
    return cluster + "." + table;
  }

  @Override
  public List<String> getClusterList() {
    LOG.info("trace getClusterList");
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
    LOG.info("trace getControllerServerList");
    try {
      return _zk.getChildren(ZookeeperPathConstants.getOnlineControllersPath(), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> getOnlineShardServers(boolean useCache, String cluster) {
    if (useCache) {
      synchronized (_onlineShardsNodes) {
        Map<String, List<String>> map = _onlineShardsNodes.get();
        if (map != null) {
          List<String> shards = map.get(cluster);
          if (shards != null) {
            return shards;
          }
        } else {
          _onlineShardsNodes.set(new ConcurrentHashMap<String, List<String>>());
          watchForOnlineShardNodes(cluster);
        }
      }
    }
    LOG.info("trace getOnlineShardServers");
    try {
      return _zk.getChildren(ZookeeperPathConstants.getClustersPath() + "/" + cluster + "/online/shard-nodes", false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void watchForOnlineShardNodes(final String cluster) {
    new WatchChildren(_zk, ZookeeperPathConstants.getOnlineShardsPath(cluster)).watch(new OnChange() {
      @Override
      public void action(List<String> children) {
        Map<String, List<String>> map = _onlineShardsNodes.get();
        map.put(cluster, children);
      }
    });
  }

  @Override
  public List<String> getShardServerList(String cluster) {
    LOG.info("trace getShardServerList");
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
      Map<String, String> map = _tableToClusterCache.get();
      if (map.containsKey(table)) {
        return true;
      } else {
        return false;
      }
    }
    LOG.info("trace exists");
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
      Boolean enabled = _enabledMap.get(getClusterTableKey(cluster, table));
      if (enabled == null) {
        throw new RuntimeException("Table [" + table + "] does not exist.");
      } else {
        return enabled;
      }
    }
    LOG.info("trace isEnabled");
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

  private Map<String, TableDescriptor> _tableDescriptorCache = new ConcurrentHashMap<String, TableDescriptor>();

  @Override
  public TableDescriptor getTableDescriptor(boolean useCache, String cluster, String table) {
    if (useCache) {
      TableDescriptor tableDescriptor = _tableDescriptorCache.get(table);
      if (tableDescriptor != null) {
        return tableDescriptor;
      }
    }
    LOG.info("trace getTableDescriptor");
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
    _tableDescriptorCache.put(table, tableDescriptor);
    return tableDescriptor;
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
  public List<String> getTableList(String cluster) {
    LOG.info("trace getTableList");
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
    clusterWatch.close();
  }

  @Override
  public String getCluster(boolean useCache, String table) {
    if (useCache) {
      Map<String, String> map = _tableToClusterCache.get();
      String cluster = map.get(table);
      if (cluster == null) {
        return null;
      } else {
        return cluster;
      }
    }
    LOG.info("trace getCluster");
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
    LOG.info("trace clearLocks");
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
  public boolean isInSafeMode(boolean useCache, String cluster) {
    if (useCache) {
      Long safeModeTimestamp = _safeModeMap.get(cluster);
      if (safeModeTimestamp == null) {
        return true;
      }
      return safeModeTimestamp < System.currentTimeMillis() ? false : true;
    }
    LOG.info("trace isInSafeMode");
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
  public int getShardCount(boolean useCache, String cluster, String table) {
    if (useCache) {
      TableDescriptor tableDescriptor = getTableDescriptor(true, cluster, table);
      return tableDescriptor.shardCount;
    }
    LOG.info("trace getShardCount");
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
    LOG.info("trace getBlockCacheFileTypes");
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
    LOG.info("trace isBlockCacheEnabled");
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
  public boolean isReadOnly(boolean useCache, String cluster, String table) {
    String key = getClusterTableKey(cluster, table);
    if (useCache) {
      Boolean flag = _readOnlyMap.get(key);
      if (flag != null) {
        return flag;
      }
    }
    LOG.info("trace isReadOnly");
    String path = ZookeeperPathConstants.getTableReadOnlyPath(cluster, table);
    Boolean flag = null;
    try {
      if (_zk.exists(path, false) == null) {
        flag = false;
        return false;
      }
      flag = true;
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
