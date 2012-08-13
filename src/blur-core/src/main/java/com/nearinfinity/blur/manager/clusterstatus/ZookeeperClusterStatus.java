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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.lucene.search.Similarity;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.ColumnPreCache;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurUtil;
import com.nearinfinity.blur.zookeeper.WatchChildren;
import com.nearinfinity.blur.zookeeper.WatchChildren.OnChange;
import com.nearinfinity.blur.zookeeper.WatchNodeData;
import com.nearinfinity.blur.zookeeper.WatchNodeExistance;

public class ZookeeperClusterStatus extends ClusterStatus {

  private static final Log LOG = LogFactory.getLog(ZookeeperClusterStatus.class);

  private ZooKeeper _zk;
  private AtomicBoolean _running = new AtomicBoolean();
  private ConcurrentMap<String, Long> _safeModeMap = new ConcurrentHashMap<String, Long>();
  private ConcurrentMap<String, Boolean> _enabledMap = new ConcurrentHashMap<String, Boolean>();
  private ConcurrentMap<String, Boolean> _readOnlyMap = new ConcurrentHashMap<String, Boolean>();
  private ConcurrentMap<String, String> _tableToClusterCache = new ConcurrentHashMap<String, String>();
  private ConcurrentMap<String, List<String>> _onlineShardsNodes = new ConcurrentHashMap<String, List<String>>();

  private WatchChildren _clusterWatcher;
  private ConcurrentMap<String, WatchChildren> _onlineShardsNodesWatchers = new ConcurrentHashMap<String, WatchChildren>();
  private ConcurrentMap<String, WatchChildren> _tableWatchers = new ConcurrentHashMap<String, WatchChildren>();
  private ConcurrentMap<String, WatchNodeExistance> _enabledTableWatchers = new ConcurrentHashMap<String, WatchNodeExistance>();
  private ConcurrentMap<String, WatchNodeExistance> _safeModeWatchers = new ConcurrentHashMap<String, WatchNodeExistance>();
  private ConcurrentMap<String, WatchNodeData> _safeModeDataWatchers = new ConcurrentHashMap<String, WatchNodeData>();

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
        if (!_tableWatchers.containsKey(cluster)) {
          WatchChildren clusterWatcher = new WatchChildren(_zk, ZookeeperPathConstants.getTablesPath(cluster)).watch(new Tables(cluster));
          _tableWatchers.put(cluster, clusterWatcher);
          WatchNodeExistance watchNodeExistance = new WatchNodeExistance(_zk, ZookeeperPathConstants.getSafemodePath(cluster)).watch(new SafeExistance(cluster));
          _safeModeWatchers.put(cluster, watchNodeExistance);
        }
      }
      List<String> clustersToCloseAndRemove = new ArrayList<String>(clusters);
      clustersToCloseAndRemove.removeAll(_tableWatchers.keySet());
      for (String cluster : clustersToCloseAndRemove) {
        WatchChildren watcher = _tableWatchers.remove(cluster);
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
        WatchNodeData watchNodeData = new WatchNodeData(_zk, ZookeeperPathConstants.getSafemodePath(cluster));
        watchNodeData.watch(new WatchNodeData.OnChange() {
          @Override
          public void action(byte[] data) {
            if (data == null) {
              LOG.info("Safe mode value for cluster [" + cluster + "] is not set.");
              _safeModeMap.put(cluster, Long.MIN_VALUE);
            } else {
              String value = new String(data);
              LOG.info("Safe mode value for cluster [" + cluster + "] is [" + value + "].");
              _safeModeMap.put(cluster, Long.parseLong(value));
            }
          }
        });
        WatchNodeData nodeData = _safeModeDataWatchers.put(cluster, watchNodeData);
        if (nodeData != null) {
          nodeData.close();
        }
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
      for (String t : tables) {
        final String table = t;
        String existingCluster = _tableToClusterCache.get(table);
        if (existingCluster == null) {
          _tableToClusterCache.put(table, cluster);
          WatchNodeExistance watchNodeExistance = new WatchNodeExistance(_zk, ZookeeperPathConstants.getTableEnabledPath(cluster, table));
          watchNodeExistance.watch(new WatchNodeExistance.OnChange() {
            @Override
            public void action(Stat stat) {
              String clusterTableKey = getClusterTableKey(cluster, table);
              if (stat == null) {
                _enabledMap.put(clusterTableKey, false);
              } else {
                _enabledMap.put(clusterTableKey, true);
              }
            }
          });
          if (_enabledTableWatchers.putIfAbsent(table, watchNodeExistance) != null) {
            watchNodeExistance.close();
          }
        } else if (!existingCluster.equals(cluster)) {
          LOG.error("Error table [{0}] is being served by more than one cluster [{1},{2}].", table, existingCluster, cluster);
        }
      }
    }
  }

  private void watchForClusters() {
    _clusterWatcher = new WatchChildren(_zk, ZookeeperPathConstants.getClustersPath()).watch(new Clusters());
  }

  public ZookeeperClusterStatus(String connectionStr) throws IOException {
    this(new ZooKeeper(connectionStr, 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    }));
  }

  private String getClusterTableKey(String cluster, String table) {
    return cluster + "." + table;
  }

  @Override
  public List<String> getClusterList() {
    long s = System.nanoTime();
    try {
      return _zk.getChildren(ZookeeperPathConstants.getClustersPath(), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace getClusterList [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public List<String> getControllerServerList() {
    long s = System.nanoTime();
    try {
      return _zk.getChildren(ZookeeperPathConstants.getOnlineControllersPath(), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace getControllerServerList [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public List<String> getOnlineShardServers(boolean useCache, String cluster) {
    if (useCache) {
      List<String> shards = _onlineShardsNodes.get(cluster);
      if (shards != null) {
        return shards;
      } else {
        watchForOnlineShardNodes(cluster);
      }
    }

    long s = System.nanoTime();
    try {
      return _zk.getChildren(ZookeeperPathConstants.getClustersPath() + "/" + cluster + "/online/shard-nodes", false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace getOnlineShardServers took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  private void watchForOnlineShardNodes(final String cluster) {
    WatchChildren watch = new WatchChildren(_zk, ZookeeperPathConstants.getOnlineShardsPath(cluster)).watch(new OnChange() {
      @Override
      public void action(List<String> children) {
        _onlineShardsNodes.put(cluster, children);
      }
    });
    if (_onlineShardsNodesWatchers.putIfAbsent(cluster, watch) != null) {
      // There was already a watch created. Close the extra watcher.
      watch.close();
    }
  }

  @Override
  public List<String> getShardServerList(String cluster) {
    long s = System.nanoTime();
    try {
      return _zk.getChildren(ZookeeperPathConstants.getClustersPath() + "/" + cluster + "/shard-nodes", false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace getShardServerList took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public boolean exists(boolean useCache, String cluster, String table) {
    if (useCache) {
      if (_tableToClusterCache.containsKey(table)) {
        return true;
      } else {
        return false;
      }
    }
    long s = System.nanoTime();
    try {
      if (_zk.exists(ZookeeperPathConstants.getTablePath(cluster, table), false) == null) {
        return false;
      }
      return true;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace exists took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public boolean isEnabled(boolean useCache, String cluster, String table) {
    if (useCache) {
      Boolean enabled = _enabledMap.get(getClusterTableKey(cluster, table));
      if (enabled != null) {
        return enabled;
      }
    }
    long s = System.nanoTime();
    String tablePathIsEnabled = ZookeeperPathConstants.getTableEnabledPath(cluster, table);
    try {
      if (_zk.exists(tablePathIsEnabled, false) == null) {
        return false;
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace isEnabled took [" + (e - s) / 1000000.0 + " ms]");
    }
    return true;
  }

  private Map<String, TableDescriptor> _tableDescriptorCache = new ConcurrentHashMap<String, TableDescriptor>();

  @Override
  public TableDescriptor getTableDescriptor(boolean useCache, String cluster, String table) {
    // if (useCache) {
    // TableDescriptor tableDescriptor = _tableDescriptorCache.get(table);
    // if (tableDescriptor != null) {
    // return tableDescriptor;
    // }
    // }
    long s = System.nanoTime();
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
      tableDescriptor.analyzerDefinition = fromBytes(getData(ZookeeperPathConstants.getTablePath(cluster, table)), AnalyzerDefinition.class);
      tableDescriptor.blockCaching = isBlockCacheEnabled(cluster, table);
      tableDescriptor.blockCachingFileTypes = getBlockCacheFileTypes(cluster, table);
      tableDescriptor.name = table;
      tableDescriptor.columnPreCache = fromBytes(getData(ZookeeperPathConstants.getTableColumnsToPreCache(cluster, table)), ColumnPreCache.class);
      byte[] data = getData(ZookeeperPathConstants.getTableSimilarityPath(cluster, table));
      if (data != null) {
        tableDescriptor.similarityClass = new String(data);
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace getTableDescriptor took [" + (e - s) / 1000000.0 + " ms]");
    }
    tableDescriptor.cluster = cluster;
    _tableDescriptorCache.put(table, tableDescriptor);
    return tableDescriptor;
  }

  @SuppressWarnings("unchecked")
  private <T extends TBase<?, ?>> T fromBytes(byte[] data, Class<T> clazz) {
    try {
      if (data == null) {
        return null;
      }
      TBase<?, ?> base = clazz.newInstance();
      TMemoryInputTransport trans = new TMemoryInputTransport(data);
      TJSONProtocol protocol = new TJSONProtocol(trans);
      base.read(protocol);
      trans.close();
      return (T) base;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
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
    long s = System.nanoTime();
    try {
      return _zk.getChildren(ZookeeperPathConstants.getTablesPath(cluster), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace getTableList took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  public void close() {
    _running.set(false);
    close(_clusterWatcher);
    close(_onlineShardsNodesWatchers);
    close(_tableWatchers);
    close(_enabledTableWatchers);
    close(_safeModeWatchers);
    close(_safeModeDataWatchers);
  }

  private void close(ConcurrentMap<String, ? extends Closeable> closableMap) {
    Collection<? extends Closeable> values = closableMap.values();
    for (Closeable closeable : values) {
      close(closeable);
    }
  }

  private void close(Closeable closeable) {
    try {
      closeable.close();
    } catch (IOException e) {
      LOG.error("Unknown error while trying to close [{0}]", closeable);
    }
  }

  @Override
  public String getCluster(boolean useCache, String table) {
    // if (useCache) {
    // Map<String, String> map = _tableToClusterCache.get();
    // String cluster = map.get(table);
    // if (cluster != null) {
    // return cluster;
    // }
    // }
    List<String> clusterList = getClusterList();
    for (String cluster : clusterList) {
      long s = System.nanoTime();
      try {
        Stat stat = _zk.exists(ZookeeperPathConstants.getTablePath(cluster, table), false);
        if (stat != null) {
          _tableToClusterCache.put(table, cluster);
          return cluster;
        }
      } catch (KeeperException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        long e = System.nanoTime();
        LOG.info("trace getCluster took [" + (e - s) / 1000000.0 + " ms]");
      }
    }
    return null;
  }

  @Override
  public void clearLocks(String cluster, String table) {
    String lockPath = ZookeeperPathConstants.getLockPath(cluster, table);
    long s = System.nanoTime();
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
    } finally {
      long e = System.nanoTime();
      LOG.info("trace clearLocks took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public boolean isInSafeMode(boolean useCache, String cluster) {
    if (useCache) {
      Long safeModeTimestamp = _safeModeMap.get(cluster);
      if (safeModeTimestamp != Long.MIN_VALUE) {
        return safeModeTimestamp < System.currentTimeMillis() ? false : true;
      }
    }
    long s = System.nanoTime();
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
    } finally {
      long e = System.nanoTime();
      LOG.info("trace isInSafeMode took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public int getShardCount(boolean useCache, String cluster, String table) {
    // if (useCache) {
    // TableDescriptor tableDescriptor = getTableDescriptor(true, cluster,
    // table);
    // return tableDescriptor.shardCount;
    // }
    long s = System.nanoTime();
    try {
      return Integer.parseInt(new String(getData(ZookeeperPathConstants.getTableShardCountPath(cluster, table))));
    } catch (NumberFormatException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace getShardCount took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public Set<String> getBlockCacheFileTypes(String cluster, String table) {
    long s = System.nanoTime();
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
    } finally {
      long e = System.nanoTime();
      LOG.info("trace getBlockCacheFileTypes took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public boolean isBlockCacheEnabled(String cluster, String table) {
    long s = System.nanoTime();
    try {
      if (_zk.exists(ZookeeperPathConstants.getTableBlockCachingFileTypesPath(cluster, table), false) == null) {
        return false;
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace isBlockCacheEnabled took [" + (e - s) / 1000000.0 + " ms]");
    }
    return true;
  }

  @Override
  public boolean isReadOnly(boolean useCache, String cluster, String table) {
    String key = getClusterTableKey(cluster, table);
    // if (useCache) {
    // Boolean flag = _readOnlyMap.get(key);
    // if (flag != null) {
    // return flag;
    // }
    // }
    long s = System.nanoTime();
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
      long e = System.nanoTime();
      LOG.info("trace isReadOnly took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public void createTable(TableDescriptor tableDescriptor) {
    long s = System.nanoTime();
    try {
      if (tableDescriptor.compressionClass == null) {
        tableDescriptor.compressionClass = DeflateCodec.class.getName();
      }
      if (tableDescriptor.similarityClass == null) {
        tableDescriptor.similarityClass = FairSimilarity.class.getName();
      }
      String table = BlurUtil.nullCheck(tableDescriptor.name, "tableDescriptor.name cannot be null.");
      String cluster = BlurUtil.nullCheck(tableDescriptor.cluster, "tableDescriptor.cluster cannot be null.");
      BlurAnalyzer analyzer = new BlurAnalyzer(BlurUtil.nullCheck(tableDescriptor.analyzerDefinition, "tableDescriptor.analyzerDefinition cannot be null."));
      String uri = BlurUtil.nullCheck(tableDescriptor.tableUri, "tableDescriptor.tableUri cannot be null.");
      int shardCount = BlurUtil.zeroCheck(tableDescriptor.shardCount, "tableDescriptor.shardCount cannot be less than 1");
      CompressionCodec compressionCodec = BlurUtil.getInstance(tableDescriptor.compressionClass, CompressionCodec.class);
      // @TODO check block size
      int compressionBlockSize = tableDescriptor.compressionBlockSize;
      Similarity similarity = BlurUtil.getInstance(tableDescriptor.similarityClass, Similarity.class);
      boolean blockCaching = tableDescriptor.blockCaching;
      Set<String> blockCachingFileTypes = tableDescriptor.blockCachingFileTypes;
      String blurTablePath = ZookeeperPathConstants.getTablePath(cluster, table);
      ColumnPreCache columnPreCache = tableDescriptor.columnPreCache;

      if (_zk.exists(blurTablePath, false) != null) {
        throw new IOException("Table [" + table + "] already exists.");
      }
      BlurUtil.setupFileSystem(uri, shardCount);
      BlurUtil.createPath(_zk, blurTablePath, analyzer.toJSON().getBytes());
      BlurUtil.createPath(_zk, ZookeeperPathConstants.getTableColumnsToPreCache(cluster, table), BlurUtil.read(columnPreCache));
      BlurUtil.createPath(_zk, ZookeeperPathConstants.getTableUriPath(cluster, table), uri.getBytes());
      BlurUtil.createPath(_zk, ZookeeperPathConstants.getTableShardCountPath(cluster, table), Integer.toString(shardCount).getBytes());
      BlurUtil.createPath(_zk, ZookeeperPathConstants.getTableCompressionCodecPath(cluster, table), compressionCodec.getClass().getName().getBytes());
      BlurUtil.createPath(_zk, ZookeeperPathConstants.getTableCompressionBlockSizePath(cluster, table), Integer.toString(compressionBlockSize).getBytes());
      BlurUtil.createPath(_zk, ZookeeperPathConstants.getTableSimilarityPath(cluster, table), similarity.getClass().getName().getBytes());
      BlurUtil.createPath(_zk, ZookeeperPathConstants.getLockPath(cluster, table), null);
      BlurUtil.createPath(_zk, ZookeeperPathConstants.getTableFieldNamesPath(cluster, table), null);
      if (tableDescriptor.readOnly) {
        BlurUtil.createPath(_zk, ZookeeperPathConstants.getTableReadOnlyPath(cluster, table), null);
      }
      if (blockCaching) {
        BlurUtil.createPath(_zk, ZookeeperPathConstants.getTableBlockCachingPath(cluster, table), null);
      }
      BlurUtil.createPath(_zk, ZookeeperPathConstants.getTableBlockCachingFileTypesPath(cluster, table), toBytes(blockCachingFileTypes));
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace createTable took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public void disableTable(String cluster, String table) {
    long s = System.nanoTime();
    try {
      if (_zk.exists(ZookeeperPathConstants.getTablePath(cluster, table), false) == null) {
        throw new IOException("Table [" + table + "] does not exist.");
      }
      String blurTableEnabledPath = ZookeeperPathConstants.getTableEnabledPath(cluster, table);
      if (_zk.exists(blurTableEnabledPath, false) == null) {
        throw new IOException("Table [" + table + "] already disabled.");
      }
      _zk.delete(blurTableEnabledPath, -1);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace disableTable took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public void enableTable(String cluster, String table) {
    long s = System.nanoTime();
    try {
      if (_zk.exists(ZookeeperPathConstants.getTablePath(cluster, table), false) == null) {
        throw new IOException("Table [" + table + "] does not exist.");
      }
      String blurTableEnabledPath = ZookeeperPathConstants.getTableEnabledPath(cluster, table);
      if (_zk.exists(blurTableEnabledPath, false) != null) {
        throw new IOException("Table [" + table + "] already enabled.");
      }
      _zk.create(blurTableEnabledPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace enableTable took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public void removeTable(String cluster, String table, boolean deleteIndexFiles) {
    long s = System.nanoTime();
    try {
      String blurTablePath = ZookeeperPathConstants.getTablePath(cluster, table);
      if (_zk.exists(blurTablePath, false) == null) {
        throw new IOException("Table [" + table + "] does not exist.");
      }
      if (_zk.exists(ZookeeperPathConstants.getTableEnabledPath(cluster, table), false) != null) {
        throw new IOException("Table [" + table + "] must be disabled before it can be removed.");
      }
      byte[] data = getData(ZookeeperPathConstants.getTableUriPath(cluster, table));
      String uri = new String(data);
      BlurUtil.removeAll(_zk, blurTablePath);
      if (deleteIndexFiles) {
        BlurUtil.removeIndexFiles(uri);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.info("trace removeTable took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  private static byte[] toBytes(Set<String> blockCachingFileTypes) {
    if (blockCachingFileTypes == null || blockCachingFileTypes.isEmpty()) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    for (String type : blockCachingFileTypes) {
      builder.append(type).append(',');
    }
    return builder.substring(0, builder.length() - 1).getBytes();
  }
}
