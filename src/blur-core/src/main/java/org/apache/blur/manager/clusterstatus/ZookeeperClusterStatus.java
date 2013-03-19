package org.apache.blur.manager.clusterstatus;

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
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.FairSimilarity;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.ColumnPreCache;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.zookeeper.WatchChildren;
import org.apache.blur.zookeeper.WatchChildren.OnChange;
import org.apache.blur.zookeeper.WatchNodeData;
import org.apache.blur.zookeeper.WatchNodeExistance;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZookeeperClusterStatus extends ClusterStatus {

  private static final Log LOG = LogFactory.getLog(ZookeeperClusterStatus.class);

  private ZooKeeper _zk;
  private AtomicBoolean _running = new AtomicBoolean();
  private ConcurrentMap<String, Long> _safeModeMap = new ConcurrentHashMap<String, Long>();
  private ConcurrentMap<String, List<String>> _onlineShardsNodes = new ConcurrentHashMap<String, List<String>>();
  private ConcurrentMap<String, Set<String>> _tablesPerCluster = new ConcurrentHashMap<String, Set<String>>();
  private AtomicReference<Set<String>> _clusters = new AtomicReference<Set<String>>(new HashSet<String>());
  private ConcurrentMap<String, Boolean> _enabled = new ConcurrentHashMap<String, Boolean>();
  private ConcurrentMap<String, Boolean> _readOnly = new ConcurrentHashMap<String, Boolean>();

  private WatchChildren _clusterWatcher;
  private ConcurrentMap<String, WatchChildren> _onlineShardsNodesWatchers = new ConcurrentHashMap<String, WatchChildren>();
  private ConcurrentMap<String, WatchChildren> _tableWatchers = new ConcurrentHashMap<String, WatchChildren>();
  private ConcurrentMap<String, WatchNodeExistance> _safeModeWatchers = new ConcurrentHashMap<String, WatchNodeExistance>();
  private ConcurrentMap<String, WatchNodeData> _safeModeDataWatchers = new ConcurrentHashMap<String, WatchNodeData>();
  private ConcurrentMap<String, WatchNodeExistance> _enabledWatchNodeExistance = new ConcurrentHashMap<String, WatchNodeExistance>();
  private ConcurrentMap<String, WatchNodeExistance> _readOnlyWatchNodeExistance = new ConcurrentHashMap<String, WatchNodeExistance>();

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
      _clusters.set(new HashSet<String>(clusters));
      for (String cluster : clusters) {
        if (!_tableWatchers.containsKey(cluster)) {
          String tablesPath = ZookeeperPathConstants.getTablesPath(cluster);
          ZkUtils.waitUntilExists(_zk, tablesPath);
          WatchChildren clusterWatcher = new WatchChildren(_zk, tablesPath).watch(new Tables(cluster));
          _tableWatchers.put(cluster, clusterWatcher);
          String safemodePath = ZookeeperPathConstants.getSafemodePath(cluster);
          ZkUtils.waitUntilExists(_zk, safemodePath);
          WatchNodeExistance watchNodeExistance = new WatchNodeExistance(_zk, safemodePath).watch(new SafeExistance(cluster));
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
              LOG.debug("Safe mode value for cluster [" + cluster + "] is not set.");
              _safeModeMap.put(cluster, Long.MIN_VALUE);
            } else {
              String value = new String(data);
              LOG.debug("Safe mode value for cluster [" + cluster + "] is [" + value + "].");
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
      Set<String> newSet = new HashSet<String>(tables);
      Set<String> oldSet = _tablesPerCluster.put(cluster, newSet);
      Set<String> newTables = getNewTables(newSet, oldSet);
      for (String table : newTables) {
        final String clusterTableKey = getClusterTableKey(cluster, table);

        WatchNodeExistance readOnlyWatcher = new WatchNodeExistance(_zk, ZookeeperPathConstants.getTableReadOnlyPath(cluster, table));
        readOnlyWatcher.watch(new WatchNodeExistance.OnChange() {
          @Override
          public void action(Stat stat) {
            if (stat == null) {
              _readOnly.put(clusterTableKey, Boolean.FALSE);
            } else {
              _readOnly.put(clusterTableKey, Boolean.TRUE);
            }
          }
        });
        if (_readOnlyWatchNodeExistance.putIfAbsent(clusterTableKey, readOnlyWatcher) != null) {
          readOnlyWatcher.close();
        }

        WatchNodeExistance enabledWatcher = new WatchNodeExistance(_zk, ZookeeperPathConstants.getTableEnabledPath(cluster, table));
        enabledWatcher.watch(new WatchNodeExistance.OnChange() {
          @Override
          public void action(Stat stat) {
            if (stat == null) {
              _enabled.put(clusterTableKey, Boolean.FALSE);
            } else {
              _enabled.put(clusterTableKey, Boolean.TRUE);
            }
          }
        });
        if (_enabledWatchNodeExistance.putIfAbsent(clusterTableKey, enabledWatcher) != null) {
          enabledWatcher.close();
        }
      }
    }

    private Set<String> getNewTables(Set<String> newSet, Set<String> oldSet) {
      Set<String> newTables = new HashSet<String>(newSet);
      if (oldSet != null) {
        newTables.removeAll(oldSet);
      }
      return newTables;
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
  public List<String> getClusterList(boolean useCache) {
    if (useCache) {
      return new ArrayList<String>(_clusters.get());
    }
    long s = System.nanoTime();
    try {
      checkIfOpen();
      return _zk.getChildren(ZookeeperPathConstants.getClustersPath(), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.debug("trace getClusterList [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  private void checkIfOpen() {
    if (_running.get()) {
      return;
    }
    throw new RuntimeException("not open");
  }

  @Override
  public List<String> getControllerServerList() {
    long s = System.nanoTime();
    try {
      checkIfOpen();
      return _zk.getChildren(ZookeeperPathConstants.getOnlineControllersPath(), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.debug("trace getControllerServerList [" + (e - s) / 1000000.0 + " ms]");
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
      checkIfOpen();
      return _zk.getChildren(ZookeeperPathConstants.getClustersPath() + "/" + cluster + "/online/shard-nodes", false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.debug("trace getOnlineShardServers took [" + (e - s) / 1000000.0 + " ms]");
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
      checkIfOpen();
      return _zk.getChildren(ZookeeperPathConstants.getClustersPath() + "/" + cluster + "/shard-nodes", false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.debug("trace getShardServerList took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public boolean exists(boolean useCache, String cluster, String table) {
    if (useCache) {
      Set<String> tables = _tablesPerCluster.get(cluster);
      if (tables != null) {
        if (tables.contains(table)) {
          return true;
        }
      }
    }
    long s = System.nanoTime();
    try {
      checkIfOpen();
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
      LOG.debug("trace exists took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public boolean isEnabled(boolean useCache, String cluster, String table) {
    if (useCache) {
      Boolean e = _enabled.get(getClusterTableKey(cluster, table));
      if (e != null) {
        return e;
      }
    }
    long s = System.nanoTime();
    String tablePathIsEnabled = ZookeeperPathConstants.getTableEnabledPath(cluster, table);
    try {
      checkIfOpen();
      if (_zk.exists(tablePathIsEnabled, false) == null) {
        return false;
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.debug("trace isEnabled took [" + (e - s) / 1000000.0 + " ms]");
    }
    return true;
  }

  private Map<String, TableDescriptor> _tableDescriptorCache = new ConcurrentHashMap<String, TableDescriptor>();

  @Override
  public TableDescriptor getTableDescriptor(boolean useCache, String cluster, String table) {
    if (useCache) {
      TableDescriptor tableDescriptor = _tableDescriptorCache.get(table);
      updateReadOnlyAndEnabled(useCache, tableDescriptor, cluster, table);
      if (tableDescriptor != null) {
        return tableDescriptor;
      }
    }
    long s = System.nanoTime();
    TableDescriptor tableDescriptor = new TableDescriptor();
    try {
      checkIfOpen();
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
      updateReadOnlyAndEnabled(useCache, tableDescriptor, cluster, table);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.debug("trace getTableDescriptor took [" + (e - s) / 1000000.0 + " ms]");
    }
    tableDescriptor.cluster = cluster;
    _tableDescriptorCache.put(table, tableDescriptor);
    return tableDescriptor;
  }

  private void updateReadOnlyAndEnabled(boolean useCache, TableDescriptor tableDescriptor, String cluster, String table) {
    if (tableDescriptor != null) {
      tableDescriptor.setReadOnly(isReadOnly(useCache, cluster, table));
      tableDescriptor.setIsEnabled(isEnabled(useCache, cluster, table));
    }
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
  public List<String> getTableList(boolean useCache, String cluster) {
    if (useCache) {
      Set<String> tables = _tablesPerCluster.get(cluster);
      if (tables != null) {
        return new ArrayList<String>(tables);
      }
    }
    long s = System.nanoTime();
    try {
      checkIfOpen();
      return _zk.getChildren(ZookeeperPathConstants.getTablesPath(cluster), false);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.debug("trace getTableList took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  public void close() {
    if (_running.get()) {
      _running.set(false);
      close(_clusterWatcher);
      close(_onlineShardsNodesWatchers);
      close(_tableWatchers);
      close(_safeModeWatchers);
      close(_safeModeDataWatchers);
      close(_enabledWatchNodeExistance);
      close(_readOnlyWatchNodeExistance);
    }
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
    if (useCache) {
      for (Entry<String, Set<String>> entry : _tablesPerCluster.entrySet()) {
        if (entry.getValue().contains(table)) {
          return entry.getKey();
        }
      }
    }
    List<String> clusterList = getClusterList(useCache);
    for (String cluster : clusterList) {
      long s = System.nanoTime();
      try {
        checkIfOpen();
        Stat stat = _zk.exists(ZookeeperPathConstants.getTablePath(cluster, table), false);
        if (stat != null) {
          // _tableToClusterCache.put(table, cluster);
          return cluster;
        }
      } catch (KeeperException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        long e = System.nanoTime();
        LOG.debug("trace getCluster took [" + (e - s) / 1000000.0 + " ms]");
      }
    }
    return null;
  }

  @Override
  public boolean isInSafeMode(boolean useCache, String cluster) {
    if (useCache) {
      Long safeModeTimestamp = _safeModeMap.get(cluster);
      if (safeModeTimestamp != null && safeModeTimestamp != Long.MIN_VALUE) {
        return safeModeTimestamp < System.currentTimeMillis() ? false : true;
      }
    }
    long s = System.nanoTime();
    try {
      checkIfOpen();
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
      LOG.debug("trace isInSafeMode took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public int getShardCount(boolean useCache, String cluster, String table) {
    if (useCache) {
      TableDescriptor tableDescriptor = getTableDescriptor(true, cluster, table);
      return tableDescriptor.shardCount;
    }
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
      LOG.debug("trace getShardCount took [" + (e - s) / 1000000.0 + " ms]");
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
      LOG.debug("trace getBlockCacheFileTypes took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public boolean isBlockCacheEnabled(String cluster, String table) {
    long s = System.nanoTime();
    try {
      checkIfOpen();
      if (_zk.exists(ZookeeperPathConstants.getTableBlockCachingFileTypesPath(cluster, table), false) == null) {
        return false;
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.debug("trace isBlockCacheEnabled took [" + (e - s) / 1000000.0 + " ms]");
    }
    return true;
  }

  @Override
  public boolean isReadOnly(boolean useCache, String cluster, String table) {
    if (useCache) {
      Boolean ro = _readOnly.get(getClusterTableKey(cluster, table));
      if (ro != null) {
        return ro;
      }
    }
    long s = System.nanoTime();
    String path = ZookeeperPathConstants.getTableReadOnlyPath(cluster, table);
    try {
      checkIfOpen();
      if (_zk.exists(path, false) == null) {
        return false;
      }
      return true;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.debug("trace isReadOnly took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public void createTable(TableDescriptor tableDescriptor) {
    long s = System.nanoTime();
    try {
      checkIfOpen();
      if (tableDescriptor.getCompressionClass() == null) {
        tableDescriptor.setCompressionClass(DefaultCodec.class.getName());
      }
      if (tableDescriptor.getSimilarityClass() == null) {
        tableDescriptor.setSimilarityClass(FairSimilarity.class.getName());
      }
      if (tableDescriptor.getAnalyzerDefinition() == null) {
        tableDescriptor.setAnalyzerDefinition(new AnalyzerDefinition());
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
      LOG.debug("trace createTable took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public void disableTable(String cluster, String table) {
    long s = System.nanoTime();
    try {
      checkIfOpen();
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
      LOG.debug("trace disableTable took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public void enableTable(String cluster, String table) {
    long s = System.nanoTime();
    try {
      checkIfOpen();
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
      LOG.debug("trace enableTable took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public void removeTable(String cluster, String table, boolean deleteIndexFiles) {
    long s = System.nanoTime();
    try {
      checkIfOpen();
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
      LOG.debug("trace removeTable took [" + (e - s) / 1000000.0 + " ms]");
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

  @Override
  public boolean isOpen() {
    return _running.get();
  }
}
