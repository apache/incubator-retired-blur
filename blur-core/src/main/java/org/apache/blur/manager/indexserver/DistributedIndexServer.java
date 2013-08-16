package org.apache.blur.manager.indexserver;

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
import static org.apache.blur.metrics.MetricsConstants.BLUR;
import static org.apache.blur.metrics.MetricsConstants.INDEX_COUNT;
import static org.apache.blur.metrics.MetricsConstants.INDEX_MEMORY_USAGE;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.SEGMENT_COUNT;
import static org.apache.blur.metrics.MetricsConstants.TABLE_COUNT;
import static org.apache.blur.utils.BlurConstants.SHARD_PREFIX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.FairSimilarity;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceFileGC;
import org.apache.blur.lucene.store.refcounter.IndexInputCloser;
import org.apache.blur.manager.BlurFilterCache;
import org.apache.blur.manager.clusterstatus.ClusterStatus;
import org.apache.blur.manager.clusterstatus.ZookeeperPathConstants;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.manager.writer.BlurIndexCloser;
import org.apache.blur.manager.writer.BlurIndexReader;
import org.apache.blur.manager.writer.BlurIndexRefresher;
import org.apache.blur.manager.writer.BlurNRTIndex;
import org.apache.blur.manager.writer.SharedMergeScheduler;
import org.apache.blur.metrics.AtomicLongGauge;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.store.blockcache.BlockDirectory;
import org.apache.blur.store.blockcache.Cache;
import org.apache.blur.store.hdfs.BlurLockFactory;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.zookeeper.WatchChildren;
import org.apache.blur.zookeeper.WatchChildren.OnChange;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;

public class DistributedIndexServer extends AbstractIndexServer {

  private static final Log LOG = LogFactory.getLog(DistributedIndexServer.class);
  private static final long _delay = TimeUnit.SECONDS.toMillis(10);
  private static final AtomicLong _pauseWarmup = new AtomicLong();

  private Map<String, TableDescriptor> _tableDescriptors = new ConcurrentHashMap<String, TableDescriptor>();
  private Map<String, Similarity> _tableSimilarity = new ConcurrentHashMap<String, Similarity>();
  private Map<String, DistributedLayout> _layoutManagers = new ConcurrentHashMap<String, DistributedLayout>();
  private Map<String, Set<String>> _layoutCache = new ConcurrentHashMap<String, Set<String>>();
  private ConcurrentHashMap<String, Map<String, BlurIndex>> _indexes = new ConcurrentHashMap<String, Map<String, BlurIndex>>();
  private final ShardStateManager _shardStateManager = new ShardStateManager();

  // set externally
  private ClusterStatus _clusterStatus;
  private Configuration _configuration;
  private String _nodeName;
  private int _shardOpenerThreadCount;
  private Cache _cache;
  private ZooKeeper _zookeeper;
  private String _cluster;

  // set internally
  private Timer _timerCacheFlush;
  private ExecutorService _openerService;
  private Timer _timerTableWarmer;
  private BlurFilterCache _filterCache;
  private AtomicBoolean _running = new AtomicBoolean();
  private long _safeModeDelay;
  private BlurIndexWarmup _warmup = new DefaultBlurIndexWarmup(1000000);
  private DirectoryReferenceFileGC _gc;
  private WatchChildren _watchOnlineShards;

  private SharedMergeScheduler _mergeScheduler;
  private IndexInputCloser _closer = null;
  private ExecutorService _searchExecutor = null;

  private AtomicLong _tableCount = new AtomicLong();
  private AtomicLong _indexCount = new AtomicLong();
  private AtomicLong _segmentCount = new AtomicLong();
  private AtomicLong _indexMemoryUsage = new AtomicLong();
  private BlurIndexRefresher _refresher;
  private BlurIndexCloser _indexCloser;
  private int _internalSearchThreads;
  private ExecutorService _warmupExecutor;
  private int _warmupThreads;

  public static interface ReleaseReader {
    void release() throws IOException;
  }

  public void init() throws KeeperException, InterruptedException, IOException {
    MetricName tableCount = new MetricName(ORG_APACHE_BLUR, BLUR, TABLE_COUNT, _cluster);
    MetricName indexCount = new MetricName(ORG_APACHE_BLUR, BLUR, INDEX_COUNT, _cluster);
    MetricName segmentCount = new MetricName(ORG_APACHE_BLUR, BLUR, SEGMENT_COUNT, _cluster);
    MetricName indexMemoryUsage = new MetricName(ORG_APACHE_BLUR, BLUR, INDEX_MEMORY_USAGE, _cluster);

    Metrics.newGauge(tableCount, new AtomicLongGauge(_tableCount));
    Metrics.newGauge(indexCount, new AtomicLongGauge(_indexCount));
    Metrics.newGauge(segmentCount, new AtomicLongGauge(_segmentCount));
    Metrics.newGauge(indexMemoryUsage, new AtomicLongGauge(_indexMemoryUsage));

    BlurUtil.setupZookeeper(_zookeeper, _cluster);
    _openerService = Executors.newThreadPool("shard-opener", _shardOpenerThreadCount);
    _gc = new DirectoryReferenceFileGC();
    _gc.init();

    // @TODO allow for configuration of these
    _mergeScheduler = new SharedMergeScheduler();
    _searchExecutor = Executors.newThreadPool("internal-search", _internalSearchThreads);
    _warmupExecutor = Executors.newThreadPool("warmup", _warmupThreads);
    _closer = new IndexInputCloser();
    _closer.init();
    _refresher = new BlurIndexRefresher();
    _refresher.init();
    _indexCloser = new BlurIndexCloser();
    _indexCloser.init();
    setupFlushCacheTimer();

    registerMyselfAsMemberOfCluster();
    String onlineShardsPath = ZookeeperPathConstants.getOnlineShardsPath(_cluster);
    String safemodePath = ZookeeperPathConstants.getSafemodePath(_cluster);
    SafeMode safeMode = new SafeMode(_zookeeper, safemodePath, onlineShardsPath, TimeUnit.MILLISECONDS, _safeModeDelay,
        TimeUnit.SECONDS, 60);
    safeMode.registerNode(getNodeName(), BlurUtil.getVersion().getBytes());

    _running.set(true);
    setupTableWarmer();
    watchForShardServerChanges();
  }

  private void watchForShardServerChanges() {
    ZookeeperPathConstants.getOnlineShardsPath(_cluster);
    _watchOnlineShards = new WatchChildren(_zookeeper, ZookeeperPathConstants.getOnlineShardsPath(_cluster))
        .watch(new OnChange() {
          private List<String> _prevOnlineShards = new ArrayList<String>();

          @Override
          public void action(List<String> onlineShards) {
            List<String> oldOnlineShards = _prevOnlineShards;
            _prevOnlineShards = onlineShards;
            _layoutManagers.clear();
            _layoutCache.clear();
            LOG.info("Online shard servers changed, clearing layout managers and cache.");
            if (oldOnlineShards == null) {
              oldOnlineShards = new ArrayList<String>();
            }
            for (String oldOnlineShard : oldOnlineShards) {
              if (!onlineShards.contains(oldOnlineShard)) {
                LOG.info("Node went offline [{0}]", oldOnlineShard);
              }
            }
            for (String onlineShard : onlineShards) {
              if (!oldOnlineShards.contains(onlineShard)) {
                LOG.info("Node came online [{0}]", onlineShard);
              }
            }
          }
        });
  }

  private void setupTableWarmer() {
    _timerTableWarmer = new Timer("Table-Warmer", true);
    _timerTableWarmer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          warmup();
        } catch (Throwable t) {
          if (_running.get()) {
            LOG.error("Unknown error", t);
          } else {
            LOG.debug("Unknown error", t);
          }
        }
      }

      private void warmup() {
        if (_running.get()) {
          List<String> tableList = _clusterStatus.getTableList(false, _cluster);
          _tableCount.set(tableList.size());
          long indexCount = 0;
          AtomicLong segmentCount = new AtomicLong();
          AtomicLong indexMemoryUsage = new AtomicLong();
          for (String table : tableList) {
            try {
              Map<String, BlurIndex> indexes = getIndexes(table);
              int count = indexes.size();
              indexCount += count;
              updateMetrics(indexes, segmentCount, indexMemoryUsage);
              LOG.debug("Table [{0}] has [{1}] number of shards online in this node.", table, count);
            } catch (IOException e) {
              LOG.error("Unknown error trying to warm table [{0}]", e, table);
            }
          }
          _indexCount.set(indexCount);
          _segmentCount.set(segmentCount.get());
          _indexMemoryUsage.set(indexMemoryUsage.get());
        }
      }

      private void updateMetrics(Map<String, BlurIndex> indexes, AtomicLong segmentCount, AtomicLong indexMemoryUsage)
          throws IOException {
        for (BlurIndex index : indexes.values()) {
          indexMemoryUsage.addAndGet(index.getIndexMemoryUsage());
          segmentCount.addAndGet(index.getSegmentCount());
        }
      }
    }, _delay, _delay);
  }

  private void registerMyselfAsMemberOfCluster() {
    String nodeName = getNodeName();
    String registeredShardsPath = ZookeeperPathConstants.getRegisteredShardsPath(_cluster) + "/" + nodeName;
    try {
      if (_zookeeper.exists(registeredShardsPath, false) == null) {
        _zookeeper.create(registeredShardsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, ShardState> getShardState(String table) {
    return _shardStateManager.getShardState(table);
  }

  private void setupFlushCacheTimer() {
    _timerCacheFlush = new Timer("Flush-IndexServer-Caches", true);
    _timerCacheFlush.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          cleanup();
        } catch (Throwable t) {
          LOG.error("Unknown error", t);
        }
      }

      private void cleanup() {
        clearMapOfOldTables(_tableDescriptors);
        clearMapOfOldTables(_layoutManagers);
        clearMapOfOldTables(_layoutCache);
        clearMapOfOldTables(_tableSimilarity);
        Map<String, Map<String, BlurIndex>> oldIndexesThatNeedToBeClosed = clearMapOfOldTables(_indexes);
        for (String table : oldIndexesThatNeedToBeClosed.keySet()) {
          Map<String, BlurIndex> indexes = oldIndexesThatNeedToBeClosed.get(table);
          if (indexes == null) {
            continue;
          }
          for (String shard : indexes.keySet()) {
            BlurIndex index = indexes.get(shard);
            if (index == null) {
              continue;
            }
            close(index, table, shard);
          }
        }
        for (String table : _indexes.keySet()) {
          Map<String, BlurIndex> shardMap = _indexes.get(table);
          if (shardMap != null) {
            Set<String> shards = new HashSet<String>(shardMap.keySet());
            Set<String> shardsToServe = getShardsToServe(table);
            shards.removeAll(shardsToServe);
            if (!shards.isEmpty()) {
              LOG.info("Need to close indexes for table [{0}] indexes [{1}]", table, shards);
            }
            for (String shard : shards) {
              LOG.info("Closing index for table [{0}] shard [{1}]", table, shard);
              BlurIndex index = shardMap.remove(shard);
              close(index, table, shard);
            }
          }
        }
      }
    }, _delay, _delay);
  }

  protected void close(BlurIndex index, String table, String shard) {
    LOG.info("Closing index [{0}] from table [{1}] shard [{2}]", index, table, shard);
    try {
      _filterCache.closing(table, shard, index);
      _shardStateManager.closing(table, shard);
      index.close();
      _shardStateManager.closed(table, shard);
    } catch (Throwable e) {
      LOG.error("Error while closing index [{0}] from table [{1}] shard [{2}]", e, index, table, shard);
      _shardStateManager.closingError(table, shard);
    }
  }

  protected <T> Map<String, T> clearMapOfOldTables(Map<String, T> map) {
    List<String> tables = new ArrayList<String>(map.keySet());
    Map<String, T> removed = new HashMap<String, T>();
    for (String table : tables) {
      if (!_clusterStatus.exists(true, _cluster, table)) {
        removed.put(table, map.remove(table));
      }
    }
    for (String table : tables) {
      if (!_clusterStatus.isEnabled(true, _cluster, table)) {
        removed.put(table, map.remove(table));
      }
    }
    return removed;
  }

  @Override
  public void close() {
    if (_running.get()) {
      _shardStateManager.close();
      _running.set(false);
      closeAllIndexes();
      _refresher.close();
      _indexCloser.close();
      _watchOnlineShards.close();
      _timerCacheFlush.purge();
      _timerCacheFlush.cancel();

      _timerTableWarmer.purge();
      _timerTableWarmer.cancel();
      _closer.close();
      _gc.close();
      _openerService.shutdownNow();
      _searchExecutor.shutdownNow();
      _warmupExecutor.shutdownNow();
    }
  }

  private void closeAllIndexes() {
    for (Entry<String, Map<String, BlurIndex>> tableToShards : _indexes.entrySet()) {
      for (Entry<String, BlurIndex> shard : tableToShards.getValue().entrySet()) {
        BlurIndex index = shard.getValue();
        try {
          index.close();
          LOG.info("Closed [{0}] [{1}] [{2}]", tableToShards.getKey(), shard.getKey(), index);
        } catch (IOException e) {
          LOG.info("Error during closing of [{0}] [{1}] [{2}]", tableToShards.getKey(), shard.getKey(), index);
        }
      }
    }
  }

  @Override
  public SortedSet<String> getShardListCurrentServerOnly(String table) throws IOException {
    return new TreeSet<String>(getShardsToServe(table));
  }

  @Override
  public Map<String, BlurIndex> getIndexes(String table) throws IOException {
    checkTable(table);

    Set<String> shardsToServe = getShardsToServe(table);
    synchronized (_indexes) {
      if (!_indexes.containsKey(table)) {
        _indexes.putIfAbsent(table, new ConcurrentHashMap<String, BlurIndex>());
      }
    }
    Map<String, BlurIndex> tableIndexes = _indexes.get(table);
    Set<String> shardsBeingServed = new HashSet<String>(tableIndexes.keySet());
    if (shardsBeingServed.containsAll(shardsToServe)) {
      Map<String, BlurIndex> result = new HashMap<String, BlurIndex>(tableIndexes);
      shardsBeingServed.removeAll(shardsToServe);
      for (String shardNotToServe : shardsBeingServed) {
        result.remove(shardNotToServe);
      }
      return result;
    } else {
      return openMissingShards(table, shardsToServe, tableIndexes);
    }
  }

  private BlurIndex openShard(String table, String shard) throws IOException {
    LOG.info("Opening shard [{0}] for table [{1}]", shard, table);
    Path tablePath = new Path(getTableDescriptor(table).tableUri);
    Path hdfsDirPath = new Path(tablePath, shard);

    BlurLockFactory lockFactory = new BlurLockFactory(_configuration, hdfsDirPath, _nodeName, BlurUtil.getPid());

    Directory directory = new HdfsDirectory(_configuration, hdfsDirPath);
    directory.setLockFactory(lockFactory);

    TableDescriptor descriptor = _clusterStatus.getTableDescriptor(true, _cluster, table);
    TableContext tableContext = TableContext.create(descriptor);
    ShardContext shardContext = ShardContext.create(tableContext, shard);

    Directory dir;
    boolean blockCacheEnabled = _clusterStatus.isBlockCacheEnabled(_cluster, table);
    if (blockCacheEnabled) {
      Set<String> blockCacheFileTypes = _clusterStatus.getBlockCacheFileTypes(_cluster, table);
      dir = new BlockDirectory(table + "_" + shard, directory, _cache, blockCacheFileTypes);
    } else {
      dir = directory;
    }

    BlurIndex index;
    if (_clusterStatus.isReadOnly(true, _cluster, table)) {
      BlurIndexReader reader = new BlurIndexReader(shardContext, dir, _refresher, _indexCloser);
      index = reader;
    } else {
      BlurNRTIndex writer = new BlurNRTIndex(shardContext, _mergeScheduler, _closer, dir, _gc, _searchExecutor);
      index = writer;
    }
    _filterCache.opening(table, shard, index);
    TableDescriptor tableDescriptor = _clusterStatus.getTableDescriptor(true, _cluster, table);
    warmUp(index, tableDescriptor, shard);
    return index;
  }

  private void warmUp(final BlurIndex index, final TableDescriptor table, final String shard) throws IOException {
    _warmupExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          final IndexSearcherClosable searcher = index.getIndexReader();
          IndexReader reader = searcher.getIndexReader();
          _warmup.warmBlurIndex(table, shard, reader, index.isClosed(), new ReleaseReader() {
            @Override
            public void release() throws IOException {
              // this will allow for closing of index
              searcher.close();
            }
          }, _pauseWarmup);
        } catch (Exception e) {
          LOG.error("Unknown error while trying to warmup index [" + index + "]", e);
        }
      }
    });

  }

  private synchronized Map<String, BlurIndex> openMissingShards(final String table, Set<String> shardsToServe,
      final Map<String, BlurIndex> tableIndexes) {
    Map<String, Future<BlurIndex>> opening = new HashMap<String, Future<BlurIndex>>();
    for (String s : shardsToServe) {
      final String shard = s;
      BlurIndex blurIndex = tableIndexes.get(shard);
      if (blurIndex == null) {
        _pauseWarmup.incrementAndGet();
        LOG.info("Opening missing shard [{0}] from table [{1}]", shard, table);
        Future<BlurIndex> submit = _openerService.submit(new Callable<BlurIndex>() {
          @Override
          public BlurIndex call() throws Exception {
            _shardStateManager.opening(table, shard);
            try {
              BlurIndex openShard = openShard(table, shard);
              _shardStateManager.open(table, shard);
              return openShard;
            } catch (Exception e) {
              _shardStateManager.openingError(table, shard);
              throw e;
            } catch (Throwable t) {
              _shardStateManager.openingError(table, shard);
              throw new RuntimeException(t);
            } finally {
              _pauseWarmup.decrementAndGet();
            }
          }
        });
        opening.put(shard, submit);
      }
    }

    for (Entry<String, Future<BlurIndex>> entry : opening.entrySet()) {
      String shard = entry.getKey();
      Future<BlurIndex> future = entry.getValue();
      try {
        BlurIndex blurIndex = future.get();
        tableIndexes.put(shard, blurIndex);
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("Unknown error while opening shard [{0}] for table [{1}].", e.getCause(), shard, table);
      }
    }

    Map<String, BlurIndex> result = new HashMap<String, BlurIndex>();
    for (String shard : shardsToServe) {
      BlurIndex blurIndex = tableIndexes.get(shard);
      if (blurIndex == null) {
        LOG.error("Missing shard [{0}] for table [{1}].", shard, table);
      } else {
        result.put(shard, blurIndex);
      }
    }
    return result;
  }

  private Set<String> getShardsToServe(String table) {
    TABLE_STATUS tableStatus = getTableStatus(table);
    if (tableStatus == TABLE_STATUS.DISABLED) {
      return new HashSet<String>();
    }
    DistributedLayout layoutManager = _layoutManagers.get(table);
    if (layoutManager == null) {
      return setupLayoutManager(table);
    } else {
      return _layoutCache.get(table);
    }
  }

  private synchronized Set<String> setupLayoutManager(String table) {
    DistributedLayoutManager layoutManager = new DistributedLayoutManager();

    String cluster = _clusterStatus.getCluster(false, table);
    if (cluster == null) {
      throw new RuntimeException("Table [" + table + "] is not found.");
    }

    List<String> shardServerList = _clusterStatus.getShardServerList(cluster);
    List<String> offlineShardServers = new ArrayList<String>(_clusterStatus.getOfflineShardServers(false, cluster));
    List<String> shardList = getShardList(table);

    layoutManager.setNodes(shardServerList);
    layoutManager.setNodesOffline(offlineShardServers);
    layoutManager.setShards(shardList);
    layoutManager.init();

    Map<String, String> layout = layoutManager.getLayout();
    String nodeName = getNodeName();
    Set<String> shardsToServeCache = new TreeSet<String>();
    for (Entry<String, String> entry : layout.entrySet()) {
      if (entry.getValue().equals(nodeName)) {
        shardsToServeCache.add(entry.getKey());
      }
    }
    _layoutCache.put(table, shardsToServeCache);
    _layoutManagers.put(table, layoutManager);
    return shardsToServeCache;
  }

  @Override
  public String getNodeName() {
    return _nodeName;
  }

  @Override
  public int getShardCount(String table) {
    checkTable(table);
    TableDescriptor descriptor = getTableDescriptor(table);
    return descriptor.shardCount;
  }

  @Override
  public List<String> getShardList(String table) {
    checkTable(table);
    List<String> result = new ArrayList<String>();
    try {
      TableDescriptor descriptor = getTableDescriptor(table);
      Path tablePath = new Path(descriptor.tableUri);
      FileSystem fileSystem = FileSystem.get(tablePath.toUri(), _configuration);
      if (!fileSystem.exists(tablePath)) {
        LOG.error("Table [{0}] is missing, defined location [{1}]", table, tablePath.toUri());
        throw new RuntimeException("Table [" + table + "] is missing, defined location [" + tablePath.toUri() + "]");
      }
      FileStatus[] listStatus = fileSystem.listStatus(tablePath);
      for (FileStatus status : listStatus) {
        if (status.isDir()) {
          String name = status.getPath().getName();
          if (name.startsWith(SHARD_PREFIX)) {
            result.add(name);
          }
        }
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Similarity getSimilarity(String table) {
    checkTable(table);
    Similarity similarity = _tableSimilarity.get(table);
    if (similarity == null) {
      TableDescriptor tableDescriptor = _clusterStatus.getTableDescriptor(true, _cluster, table);
      String similarityClass = tableDescriptor.similarityClass;
      if (similarityClass == null) {
        similarity = new FairSimilarity();
      } else {
        similarity = getInstance(similarityClass, Similarity.class);
      }
      _tableSimilarity.put(table, similarity);
    }
    return similarity;
  }

  @Override
  public long getTableSize(String table) throws IOException {
    checkTable(table);
    Path tablePath = new Path(getTableUri(table));
    FileSystem fileSystem = FileSystem.get(tablePath.toUri(), _configuration);
    ContentSummary contentSummary = fileSystem.getContentSummary(tablePath);
    return contentSummary.getLength();
  }

  @Override
  public TABLE_STATUS getTableStatus(String table) {
    checkTable(table);
    boolean enabled = _clusterStatus.isEnabled(true, _cluster, table);
    if (enabled) {
      return TABLE_STATUS.ENABLED;
    }
    return TABLE_STATUS.DISABLED;
  }

  private void checkTable(String table) {
    if (_clusterStatus.exists(true, _cluster, table)) {
      return;
    }
    throw new RuntimeException("Table [" + table + "] does not exist.");
  }

  @Override
  public String getTableUri(String table) {
    checkTable(table);
    TableDescriptor descriptor = getTableDescriptor(table);
    return descriptor.tableUri;
  }

  private TableDescriptor getTableDescriptor(String table) {
    TableDescriptor tableDescriptor = _tableDescriptors.get(table);
    if (tableDescriptor == null) {
      tableDescriptor = _clusterStatus.getTableDescriptor(true, _cluster, table);
      _tableDescriptors.put(table, tableDescriptor);
    }
    return tableDescriptor;
  }

  @SuppressWarnings("unchecked")
  private <T> T getInstance(String className, Class<T> c) {
    try {
      Class<? extends T> clazz = (Class<? extends T>) Class.forName(className);
      Object object = clazz.newInstance();
      if (object instanceof Configurable) {
        Configurable configurable = (Configurable) object;
        configurable.setConf(_configuration);
      }
      return (T) object;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void setClusterStatus(ClusterStatus clusterStatus) {
    _clusterStatus = clusterStatus;
  }

  public void setConfiguration(Configuration configuration) {
    _configuration = configuration;
  }

  public void setNodeName(String nodeName) {
    _nodeName = nodeName;
  }

  public void setShardOpenerThreadCount(int shardOpenerThreadCount) {
    _shardOpenerThreadCount = shardOpenerThreadCount;
  }

  public void setCache(Cache cache) {
    _cache = cache;
  }

  public void setZookeeper(ZooKeeper zookeeper) {
    _zookeeper = zookeeper;
  }

  public void setFilterCache(BlurFilterCache filterCache) {
    _filterCache = filterCache;
  }

  public void setSafeModeDelay(long safeModeDelay) {
    _safeModeDelay = safeModeDelay;
  }

  public void setWarmup(BlurIndexWarmup warmup) {
    _warmup = warmup;
  }

  public void setClusterName(String cluster) {
    _cluster = cluster;
  }

  public void setInternalSearchThreads(int internalSearchThreads) {
    _internalSearchThreads = internalSearchThreads;
  }

  public void setWarmupThreads(int warmupThreads) {
    _warmupThreads = warmupThreads;
  }

  public static AtomicLong getPauseWarmup() {
    return _pauseWarmup;
  }
}
