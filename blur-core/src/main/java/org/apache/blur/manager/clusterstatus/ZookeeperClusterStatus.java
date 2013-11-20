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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.FairSimilarity;
import org.apache.blur.thirdparty.thrift_0_9_0.TDeserializer;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.TSerializer;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TJSONProtocol;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.zookeeper.WatchChildren;
import org.apache.blur.zookeeper.WatchChildren.OnChange;
import org.apache.blur.zookeeper.WatchNodeData;
import org.apache.blur.zookeeper.WatchNodeExistance;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.blur.zookeeper.ZooKeeperLockManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZookeeperClusterStatus extends ClusterStatus {

  private static final Log LOG = LogFactory.getLog(ZookeeperClusterStatus.class);

  private final ZooKeeper _zk;
  private final BlurConfiguration _configuration;
  private final AtomicBoolean _running = new AtomicBoolean();
  private final ConcurrentMap<String, Long> _safeModeMap = new ConcurrentHashMap<String, Long>();
  private final ConcurrentMap<String, List<String>> _onlineShardsNodes = new ConcurrentHashMap<String, List<String>>();
  private final ConcurrentMap<String, Set<String>> _tablesPerCluster = new ConcurrentHashMap<String, Set<String>>();
  private final AtomicReference<Set<String>> _clusters = new AtomicReference<Set<String>>(new HashSet<String>());
  private final ConcurrentMap<String, Boolean> _enabled = new ConcurrentHashMap<String, Boolean>();

  private final WatchChildren _clusterWatcher;
  private final ConcurrentMap<String, WatchChildren> _onlineShardsNodesWatchers = new ConcurrentHashMap<String, WatchChildren>();
  private final ConcurrentMap<String, WatchChildren> _tableWatchers = new ConcurrentHashMap<String, WatchChildren>();
  private final Map<String, SafeModeCacheEntry> _clusterToSafeMode = new ConcurrentHashMap<String, ZookeeperClusterStatus.SafeModeCacheEntry>();
  private final ConcurrentMap<String, WatchNodeExistance> _enabledWatchNodeExistance = new ConcurrentHashMap<String, WatchNodeExistance>();

  public ZookeeperClusterStatus(ZooKeeper zooKeeper, BlurConfiguration configuration) {
    _zk = zooKeeper;
    _running.set(true);
    _clusterWatcher = watchForClusters();
    _configuration = configuration;
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public ZookeeperClusterStatus(String connectionStr, BlurConfiguration configuration) throws IOException {
    this(new ZooKeeper(connectionStr, 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    }), configuration);
  }

  public ZookeeperClusterStatus(ZooKeeper zooKeeper) throws IOException {
    this(zooKeeper, new BlurConfiguration());
  }

  public ZookeeperClusterStatus(String connectionStr) throws IOException {
    this(connectionStr, new BlurConfiguration());
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
      Set<String> oldTables = getOldTables(newSet, oldSet);
      for (String table : oldTables) {
        final String clusterTableKey = getClusterTableKey(cluster, table);
        WatchNodeExistance watchNodeExistance = _enabledWatchNodeExistance.remove(clusterTableKey);
        if (watchNodeExistance != null) {
          watchNodeExistance.close();  
        }
        _tableDescriptorCache.remove(table);
      }
      for (String table : newTables) {
        final String clusterTableKey = getClusterTableKey(cluster, table);
        WatchNodeExistance enabledWatcher = new WatchNodeExistance(_zk, ZookeeperPathConstants.getTableEnabledPath(
            cluster, table));
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

    private Set<String> getOldTables(Set<String> newSet, Set<String> oldSet) {
      Set<String> oldTables = new HashSet<String>();
      if (oldSet != null) {
        oldTables.addAll(oldSet);
      }
      if (newSet != null) {
        oldTables.removeAll(newSet);
      }
      return oldTables;
    }

    private Set<String> getNewTables(Set<String> newSet, Set<String> oldSet) {
      Set<String> newTables = new HashSet<String>(newSet);
      if (oldSet != null) {
        newTables.removeAll(oldSet);
      }
      return newTables;
    }
  }

  private WatchChildren watchForClusters() {
    return new WatchChildren(_zk, ZookeeperPathConstants.getClustersPath()).watch(new Clusters());
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
  public List<String> getOnlineControllerList() {
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
      LOG.debug("trace getOnlineControllerList [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  @Override
  public List<String> getControllerServerList() {
    long s = System.nanoTime();
    try {
      checkIfOpen();
      return _zk.getChildren(ZookeeperPathConstants.getControllersPath(), false);
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
      String onlineShardsPath = ZookeeperPathConstants.getOnlineShardsPath(cluster);
      return _zk.getChildren(onlineShardsPath, false);
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
    WatchChildren watch = new WatchChildren(_zk, ZookeeperPathConstants.getOnlineShardsPath(cluster))
        .watch(new OnChange() {
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
      String registeredShardsPath = ZookeeperPathConstants.getRegisteredShardsPath(cluster);
      return _zk.getChildren(registeredShardsPath, false);
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
      updateEnabled(useCache, tableDescriptor, cluster, table);
      if (tableDescriptor != null) {
        return tableDescriptor;
      }
    }
    long s = System.nanoTime();
    TableDescriptor tableDescriptor = new TableDescriptor();
    try {
      checkIfOpen();
      
      String blurTablePath = ZookeeperPathConstants.getTablePath(cluster, table);
      byte[] bytes = getData(blurTablePath);
      
      if (bytes == null || bytes.length == 0) {
        /*
         * table descriptor is stored in an older format where we manually
         * serialized each field into a different zookeeper node
         * so we fetch it using old code and serialize it again with thrift protocol
         */
        LOG.info("The schema of Table [{0}] was stored in an older format. Now converting it to the new format", table);
        getOldTableDescriptor(useCache, cluster, table, tableDescriptor);
        
        BlurUtil.removeAll(_zk, blurTablePath);
        
        // store it using thrift protocol
        byte[] newFormatBytes = serializeTableDescriptor(tableDescriptor);
        BlurUtil.createPath(_zk, blurTablePath, newFormatBytes);
        
      } else {
        TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
        deserializer.deserialize(tableDescriptor, bytes);
      }
    } catch (TException e) { 
      throw new RuntimeException(e);
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

  private TableDescriptor getOldTableDescriptor(boolean useCache, String cluster, String table, TableDescriptor tableDescriptor) {
    long s = System.nanoTime();
    try {
      
      NullPointerException npe = null;
      LOOP: for (int i = 0; i < 10; i++) {
        npe = null;
        try {
          tableDescriptor.shardCount = Integer.parseInt(new String(getData(ZookeeperPathConstants
              .getTableShardCountPath(cluster, table))));
          tableDescriptor.tableUri = new String(getData(ZookeeperPathConstants.getTableUriPath(cluster, table)));
          tableDescriptor.blockCaching = isBlockCacheEnabled(cluster, table);
          tableDescriptor.blockCachingFileTypes = getBlockCacheFileTypes(cluster, table);
          tableDescriptor.name = table;
          tableDescriptor.readOnly = internalGetReadOnly(ZookeeperPathConstants.getTableReadOnlyPath(cluster, table));
          tableDescriptor.preCacheCols = toList(getData(ZookeeperPathConstants
              .getTableColumnsToPreCache(cluster, table)));
          byte[] data = getData(ZookeeperPathConstants.getTableSimilarityPath(cluster, table));
          if (data != null) {
            tableDescriptor.similarityClass = new String(data);
          }
          updateEnabled(useCache, tableDescriptor, cluster, table);
          break LOOP;
        } catch (NullPointerException e) {
          npe = e;
          LOG.warn("Terrible hack to make the table admins pick up on changes to table descriptors while it's being created.");
          Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        }
      }
      if (npe != null) {
        throw npe;
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.debug("trace getOldTableDescriptor took [" + (e - s) / 1000000.0 + " ms]");
    }
    
    return tableDescriptor;
  }
  
  private boolean internalGetReadOnly(String tableReadOnlyPath) throws KeeperException, InterruptedException {
    Stat stat = _zk.exists(tableReadOnlyPath, false);
    if (stat == null) {
      return false;
    }
    return true;
  }

  private static List<String> toList(byte[] bs) {
    if (bs == null) {
      return null;
    }
    String str = new String(bs);
    String[] split = str.split(",");
    List<String> list = new ArrayList<String>();
    for (String s : split) {
      list.add(s.trim());
    }
    return list;
  }

  private void updateEnabled(boolean useCache, TableDescriptor tableDescriptor, String cluster, String table) {
    if (tableDescriptor != null) {
      tableDescriptor.setEnabled(isEnabled(useCache, cluster, table));
    }
  }

  private byte[] getData(String path) throws KeeperException, InterruptedException {
    Stat stat = _zk.exists(path, false);
    if (stat == null) {
      LOG.debug("Tried to fetch path [{0}] and path is missing", path);
      return null;
    }
    byte[] data = _zk.getData(path, false, stat);
    if (data == null) {
      LOG.debug("Fetched path [{0}] and data is null", path);
      return null;
    }
    return data;
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
      close(_enabledWatchNodeExistance);
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

  static class SafeModeCacheEntry {
    static final long _10_SECONDS = TimeUnit.SECONDS.toMillis(10);
    boolean _safeMode;
    long _lastCheck;

    public SafeModeCacheEntry(boolean safeMode) {
      _lastCheck = System.currentTimeMillis();
      _safeMode = safeMode;
    }

    boolean isValid() {
      long now = System.currentTimeMillis();
      if (_lastCheck + _10_SECONDS < now) {
        return false;
      }
      return true;
    }
  }

  @Override
  public boolean isInSafeMode(boolean useCache, String cluster) {
    if (useCache) {
      SafeModeCacheEntry safeModeCacheEntry = _clusterToSafeMode.get(cluster);
      if (safeModeCacheEntry != null && safeModeCacheEntry.isValid()) {
        return safeModeCacheEntry._safeMode;
      }
    }
    long s = System.nanoTime();
    try {
      checkIfOpen();
      String safemodePath = ZookeeperPathConstants.getSafemodePath(cluster);
      ZooKeeperLockManager zooKeeperLockManager = new ZooKeeperLockManager(_zk, safemodePath);
      if (zooKeeperLockManager.getNumberOfLockNodesPresent(cluster) == 0) {
        _clusterToSafeMode.put(cluster, new SafeModeCacheEntry(false));
        return false;
      }
      return true;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      long e = System.nanoTime();
      LOG.debug("trace isInSafeMode took [" + (e - s) / 1000000.0 + " ms]");
    }
  }

  private Set<String> getBlockCacheFileTypes(String cluster, String table) {
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

  private boolean isBlockCacheEnabled(String cluster, String table) {
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
    TableDescriptor tableDescriptor = getTableDescriptor(useCache, cluster, table);
    return tableDescriptor.isReadOnly();
  }

  @Override
  public void createTable(TableDescriptor tableDescriptor) {
    long s = System.nanoTime();
    try {
      checkIfOpen();
      if (tableDescriptor.getSimilarityClass() == null) {
        tableDescriptor.setSimilarityClass(FairSimilarity.class.getName());
      }
      String table = BlurUtil.nullCheck(tableDescriptor.name, "tableDescriptor.name cannot be null.");
      String cluster = BlurUtil.nullCheck(tableDescriptor.cluster, "tableDescriptor.cluster cannot be null.");
      assignTableUri(tableDescriptor);
      String uri = BlurUtil.nullCheck(tableDescriptor.tableUri, "tableDescriptor.tableUri cannot be null.");
      int shardCount = BlurUtil.zeroCheck(tableDescriptor.shardCount,
          "tableDescriptor.shardCount cannot be less than 1");
      String blurTablePath = ZookeeperPathConstants.getTablePath(cluster, table);
      if (_zk.exists(blurTablePath, false) != null) {
        throw new IOException("Table [" + table + "] already exists.");
      }
      BlurUtil.setupFileSystem(uri, shardCount);
      byte[] bytes = serializeTableDescriptor(tableDescriptor); 
      BlurUtil.createPath(_zk, blurTablePath, bytes);
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

  private byte[] serializeTableDescriptor(TableDescriptor td) {
    try{
      TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
      return serializer.serialize(td);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }
  
  private void assignTableUri(TableDescriptor tableDescriptor) {
    if (tableDescriptor.getTableUri() != null) {
      return;
    }
    String cluster = tableDescriptor.getCluster();
    String defaultTableUriPropertyName = BlurConstants.getDefaultTableUriPropertyName(cluster);
    String parentPath = _configuration.get(defaultTableUriPropertyName);
    if (parentPath == null) {
      return;
    }
    String tableUri = parentPath + "/" + tableDescriptor.getName();
    LOG.info("Setting default table uri for table [{0}] of [{1}]", tableDescriptor.getName(), tableUri);
    tableDescriptor.setTableUri(tableUri);
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
      TableDescriptor tableDescriptor = getTableDescriptor(true, cluster, table);
      String blurTablePath = ZookeeperPathConstants.getTablePath(cluster, table);
      if (_zk.exists(blurTablePath, false) == null) {
        throw new IOException("Table [" + table + "] does not exist.");
      }
      if (_zk.exists(ZookeeperPathConstants.getTableEnabledPath(cluster, table), false) != null) {
        throw new IOException("Table [" + table + "] must be disabled before it can be removed.");
      }
      String uri = tableDescriptor.getTableUri();
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

  @Override
  public boolean isOpen() {
    return _running.get();
  }
}
