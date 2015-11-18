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
import static org.apache.blur.utils.BlurConstants.BLUR_TABLE_DISABLE_FAST_DIR;

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
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.BlurFilterCache;
import org.apache.blur.manager.clusterstatus.ClusterStatus;
import org.apache.blur.manager.clusterstatus.ClusterStatus.Action;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.manager.writer.BlurIndexCloser;
import org.apache.blur.manager.writer.BlurIndexReadOnly;
import org.apache.blur.manager.writer.SharedMergeScheduler;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.server.cache.ThriftCache;
import org.apache.blur.store.BlockCacheDirectoryFactory;
import org.apache.blur.store.hdfs.BlurLockFactory;
import org.apache.blur.store.hdfs.DirectoryUtil;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.store.hdfs.SequentialReadControl;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.ShardUtil;
import org.apache.blur.zookeeper.WatchChildren;
import org.apache.blur.zookeeper.WatchChildren.OnChange;
import org.apache.blur.zookeeper.ZookeeperPathConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.common.io.Closer;

public class DistributedIndexServer extends AbstractDistributedIndexServer {

  private static final Log LOG = LogFactory.getLog(DistributedIndexServer.class);
  private static final long _delay = TimeUnit.SECONDS.toMillis(10);
  private static final AtomicLong _pauseWarmup = new AtomicLong();
  private static final Set<String> EMPTY = new HashSet<String>();

  static class LayoutEntry {

    LayoutEntry(DistributedLayout distributedLayout, Set<String> shards) {
      _distributedLayout = distributedLayout;
      _shards = shards;
    }

    final DistributedLayout _distributedLayout;
    final Set<String> _shards;
  }

  // set externally
  private final int _shardOpenerThreadCount;
  private final BlockCacheDirectoryFactory _blockCacheDirectoryFactory;
  private final ZooKeeper _zookeeper;
  private final int _internalSearchThreads;
  private final long _safeModeDelay;
  private final BlurFilterCache _filterCache;
  private final DistributedLayoutFactory _distributedLayoutFactory;

  // set internally
  private final AtomicBoolean _running = new AtomicBoolean();
  private final Thread _timerTableWarmer;
  private final Object _warmupLock = new Object();
  private final Thread _timerCacheFlush;
  private final Object _cleanupLock = new Object();
  private final ExecutorService _openerService;
  private final WatchChildren _watchOnlineShards;
  private final SharedMergeScheduler _mergeScheduler;
  private final ExecutorService _searchExecutor;
  private final BlurIndexCloser _indexCloser;
  private final ConcurrentMap<String, LayoutEntry> _layout = new ConcurrentHashMap<String, LayoutEntry>();
  private final ConcurrentMap<String, Map<String, BlurIndex>> _indexes = new ConcurrentHashMap<String, Map<String, BlurIndex>>();
  private final ShardStateManager _shardStateManager = new ShardStateManager();
  private final Closer _closer;
  private long _shortDelay = 250;
  private final int _minimumNumberOfNodes;
  private final Timer _hdfsKeyValueTimer;
  private final Timer _indexImporterTimer;
  private final Timer _indexBulkTimer;
  private final Timer _indexIdleWriterTimer;
  private final ThriftCache _thriftCache;
  private final SequentialReadControl _sequentialReadControl;
  private final long _maxWriterIdle;

  public DistributedIndexServer(Configuration configuration, ZooKeeper zookeeper, ClusterStatus clusterStatus,
      BlurFilterCache filterCache, BlockCacheDirectoryFactory blockCacheDirectoryFactory,
      DistributedLayoutFactory distributedLayoutFactory, String cluster, String nodeName, long safeModeDelay,
      int shardOpenerThreadCount, int maxMergeThreads, int internalSearchThreads,
      int minimumNumberOfNodesBeforeExitingSafeMode, Timer hdfsKeyValueTimer, Timer indexImporterTimer,
      long smallMergeThreshold, Timer indexBulkTimer, ThriftCache thriftCache,
      SequentialReadControl sequentialReadControl, Timer indexIdleWriterTimer, long maxWriterIdle)
      throws KeeperException, InterruptedException {
    super(clusterStatus, configuration, nodeName, cluster);
    _indexIdleWriterTimer = indexIdleWriterTimer;
    _maxWriterIdle = maxWriterIdle;
    _sequentialReadControl = sequentialReadControl;
    _indexImporterTimer = indexImporterTimer;
    _indexBulkTimer = indexBulkTimer;
    _hdfsKeyValueTimer = hdfsKeyValueTimer;
    _minimumNumberOfNodes = minimumNumberOfNodesBeforeExitingSafeMode;
    _running.set(true);
    _closer = Closer.create();
    _shardOpenerThreadCount = shardOpenerThreadCount;
    _zookeeper = zookeeper;
    _filterCache = filterCache;
    _safeModeDelay = safeModeDelay;
    _internalSearchThreads = internalSearchThreads;
    _blockCacheDirectoryFactory = blockCacheDirectoryFactory;
    _distributedLayoutFactory = distributedLayoutFactory;
    _thriftCache = thriftCache;

    _closer.register(_shardStateManager);

    BlurUtil.setupZookeeper(_zookeeper, _cluster);
    _openerService = Executors.newThreadPool("shard-opener", _shardOpenerThreadCount);
    _searchExecutor = Executors.newThreadPool("internal-search", _internalSearchThreads);

    _closer.register(CloseableExecutorService.close(_openerService));
    _closer.register(CloseableExecutorService.close(_searchExecutor));

    // @TODO allow for configuration of these
    _mergeScheduler = _closer.register(new SharedMergeScheduler(maxMergeThreads, smallMergeThreshold));

    _indexCloser = _closer.register(new BlurIndexCloser());
    _timerCacheFlush = setupFlushCacheTimer();
    _timerCacheFlush.start();

    String onlineShardsPath = ZookeeperPathConstants.getOnlineShardsPath(_cluster);
    String safemodePath = ZookeeperPathConstants.getSafemodePath(_cluster);

    // Set the registerNode timeout value to zk sessionTimeout + {4} seconds
    int registerNodeTimeOut = _zookeeper.getSessionTimeout() / 1000 + 4;

    SafeMode safeMode = new SafeMode(_zookeeper, safemodePath, onlineShardsPath, TimeUnit.MILLISECONDS, _safeModeDelay,
        TimeUnit.SECONDS, registerNodeTimeOut, _minimumNumberOfNodes);
    safeMode.registerNode(getNodeName(), BlurUtil.getVersion().getBytes());

    _timerTableWarmer = setupTableWarmer();
    _timerTableWarmer.start();
    _watchOnlineShards = watchForShardServerChanges();
    _clusterStatus.registerActionOnTableStateChange(new Action() {
      @Override
      public void action() {
        synchronized (_warmupLock) {
          _warmupLock.notifyAll();
        }
      }
    });
    _clusterStatus.registerActionOnTableStateChange(new Action() {
      @Override
      public void action() {
        synchronized (_cleanupLock) {
          _cleanupLock.notifyAll();
        }
      }
    });
  }

  @Override
  public void close() throws IOException {
    if (_running.get()) {
      _running.set(false);
      _closer.close();
      closeAllIndexes();
      _timerCacheFlush.interrupt();
      _watchOnlineShards.close();
      _timerTableWarmer.interrupt();
    }
  }

  public static interface ReleaseReader {
    void release() throws IOException;
  }

  public static AtomicLong getPauseWarmup() {
    return _pauseWarmup;
  }

  @Override
  public Map<String, ShardState> getShardState(String table) {
    return _shardStateManager.getShardState(table);
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

  private boolean isEnabled(String table) {
    checkTable(table);
    return _clusterStatus.isEnabled(true, _cluster, table);
  }

  private WatchChildren watchForShardServerChanges() {
    WatchChildren watchOnlineShards = new WatchChildren(_zookeeper,
        ZookeeperPathConstants.getOnlineShardsPath(_cluster));
    watchOnlineShards.watch(new OnChange() {
      private List<String> _prevOnlineShards = new ArrayList<String>();

      @Override
      public void action(List<String> onlineShards) {
        List<String> oldOnlineShards = _prevOnlineShards;
        _prevOnlineShards = onlineShards;
        _layout.clear();
        LOG.info("Layouts cleared, possible node change or rebalance.");
        boolean change = false;
        if (oldOnlineShards == null) {
          oldOnlineShards = new ArrayList<String>();
        }
        for (String oldOnlineShard : oldOnlineShards) {
          if (!onlineShards.contains(oldOnlineShard)) {
            LOG.info("Node went offline [{0}]", oldOnlineShard);
            change = true;
          }
        }
        for (String onlineShard : onlineShards) {
          if (!oldOnlineShards.contains(onlineShard)) {
            LOG.info("Node came online [{0}]", onlineShard);
            change = true;
          }
        }
        if (change) {
          LOG.info("Online shard servers changed, clearing layout managers and cache.");
        }
      }
    });
    return _closer.register(watchOnlineShards);
  }

  private Thread setupTableWarmer() {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        runWarmup();
        while (_running.get()) {
          long s = System.nanoTime();
          synchronized (_warmupLock) {
            try {
              _warmupLock.wait(_delay);
            } catch (InterruptedException ex) {
              return;
            }
          }
          long e = System.nanoTime();
          if ((e - s) < TimeUnit.MILLISECONDS.toNanos(_delay)) {
            runWarmup();
          } else {
            for (int i = 0; i < 10; i++) {
              runWarmup();
              synchronized (_warmupLock) {
                try {
                  _warmupLock.wait(_shortDelay);
                } catch (InterruptedException ex) {
                  return;
                }
              }
            }
          }
        }
      }
    });
    thread.setDaemon(true);
    thread.setName("Table-Warmer");
    return thread;
  }

  private Thread setupFlushCacheTimer() {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          synchronized (_cleanupLock) {
            try {
              _cleanupLock.wait(_delay);
            } catch (InterruptedException e) {
              return;
            }
          }
          for (int i = 0; i < 10; i++) {
            runCleanup();
            synchronized (_cleanupLock) {
              try {
                _cleanupLock.wait(_shortDelay);
              } catch (InterruptedException e) {
                return;
              }
            }
          }
        }
      }
    });
    thread.setDaemon(true);
    thread.setName("Flush-IndexServer-Caches");
    return thread;
  }

  private synchronized void runCleanup() {
    try {
      cleanup();
    } catch (Throwable t) {
      if (_running.get()) {
        LOG.error("Unknown error", t);
      } else {
        LOG.debug("Unknown error", t);
      }
    }
  }

  private synchronized void runWarmup() {
    try {
      LOG.debug("Running warmup on the indexes.");
      warmupTables();
    } catch (Throwable t) {
      if (_running.get()) {
        LOG.error("Unknown error", t);
      } else {
        LOG.debug("Unknown error", t);
      }
    }
  }

  private void warmupTables() {
    if (_running.get()) {
      List<String> tableList = _clusterStatus.getTableList(false, _cluster);
      _tableCount.set(tableList.size());
      long indexCount = 0;
      AtomicLong segmentCount = new AtomicLong();
      AtomicLong indexMemoryUsage = new AtomicLong();
      AtomicLong recordCount = new AtomicLong();
      for (String table : tableList) {
        try {
          Map<String, BlurIndex> indexes = getIndexes(table);
          int count = indexes.size();
          indexCount += count;
          updateMetrics(indexes, segmentCount, indexMemoryUsage, recordCount);
          LOG.debug("Table [{0}] has [{1}] number of shards online in this node.", table, count);
        } catch (IOException e) {
          LOG.error("Unknown error trying to warm table [{0}]", e, table);
        }
      }
      _indexCount.set(indexCount);
      _segmentCount.set(segmentCount.get());
      _indexMemoryUsage.set(indexMemoryUsage.get());
      _recordCount.set(recordCount.get());
    }
  }

  private void updateMetrics(Map<String, BlurIndex> indexes, AtomicLong segmentCount, AtomicLong indexMemoryUsage,
      AtomicLong recordCount) throws IOException {
    for (BlurIndex index : indexes.values()) {
      indexMemoryUsage.addAndGet(index.getIndexMemoryUsage());
      segmentCount.addAndGet(index.getSegmentCount());
      recordCount.addAndGet(index.getRecordCount());
    }
  }

  private void cleanup() {
    clearMapOfOldTables(_layout);
    clearMapOfOldTables(_distributedLayoutFactory.getLayoutCache());
    boolean closed = false;
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
        closed = true;
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
          closed = true;
        }
      }
      if (closed) {
        TableContext.clear(table);
      }
    }
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

  private BlurIndex openShard(String table, String shard) throws IOException {
    LOG.info("Opening shard [{0}] for table [{1}]", shard, table);
    TableContext tableContext = getTableContext(table);
    Path tablePath = tableContext.getTablePath();
    Path hdfsDirPath = new Path(tablePath, shard);

    BlurLockFactory lockFactory = new BlurLockFactory(_configuration, hdfsDirPath, _nodeName, BlurUtil.getPid());
    HdfsDirectory longTermStorage = new HdfsDirectory(_configuration, hdfsDirPath, _sequentialReadControl);
    longTermStorage.setLockFactory(lockFactory);

    boolean disableFast = tableContext.getBlurConfiguration().getBoolean(BLUR_TABLE_DISABLE_FAST_DIR, false);
    Directory directory = DirectoryUtil.getDirectory(_configuration, longTermStorage, disableFast, _hdfsKeyValueTimer,
        table, shard, false);
    ShardContext shardContext = ShardContext.create(tableContext, shard);

    TableDescriptor descriptor = tableContext.getDescriptor();
    boolean blockCacheEnabled = descriptor.isBlockCaching();
    if (blockCacheEnabled) {
      Set<String> blockCacheFileTypes = descriptor.getBlockCachingFileTypes();
      directory = _blockCacheDirectoryFactory.newDirectory(table, shard, directory, blockCacheFileTypes);
    }

    BlurIndex index = tableContext.newInstanceBlurIndex(shardContext, directory, _mergeScheduler, _searchExecutor,
        _indexCloser, _indexImporterTimer, _indexBulkTimer, _thriftCache, _indexIdleWriterTimer, _maxWriterIdle);

    if (_clusterStatus.isReadOnly(true, _cluster, table)) {
      index = new BlurIndexReadOnly(index);
    }
    _filterCache.opening(table, shard, index);
    return index;
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
    if (!isEnabled(table)) {
      return EMPTY;
    }
    LayoutEntry layoutEntry = _layout.get(table);
    if (layoutEntry == null) {
      return setupLayoutManager(table);
    } else {
      return layoutEntry._shards;
    }
  }

  private synchronized Set<String> setupLayoutManager(String table) {
    String cluster = _clusterStatus.getCluster(false, table);
    if (cluster == null) {
      throw new RuntimeException("Table [" + table + "] is not found.");
    }
    List<String> onlineShardServerList = _clusterStatus.getOnlineShardServers(false, cluster);
    TableDescriptor tableDescriptor = _clusterStatus.getTableDescriptor(false, cluster, table);
    List<String> shardList = generateShardList(tableDescriptor);

    String shutdownPath = ZookeeperPathConstants.getShutdownPath(cluster);
    if (isShuttingDown(shutdownPath)) {
      LOG.info("Cluster shutting down, return empty layout.");
      return EMPTY;
    }

    DistributedLayout layoutManager = _distributedLayoutFactory.createDistributedLayout(table, shardList,
        onlineShardServerList);

    Map<String, String> layout = layoutManager.getLayout();
    String nodeName = getNodeName();
    Set<String> shardsToServeCache = new TreeSet<String>();
    for (Entry<String, String> entry : layout.entrySet()) {
      if (entry.getValue().equals(nodeName)) {
        shardsToServeCache.add(entry.getKey());
      }
    }
    _layout.put(table, new LayoutEntry(layoutManager, shardsToServeCache));
    return shardsToServeCache;
  }

  private List<String> generateShardList(TableDescriptor tableDescriptor) {
    int shardCount = tableDescriptor.getShardCount();
    List<String> list = new ArrayList<String>();
    for (int i = 0; i < shardCount; i++) {
      list.add(ShardUtil.getShardName(i));
    }
    return list;
  }

  private boolean isShuttingDown(String shutdownPath) {
    try {
      Stat stat = _zookeeper.exists(shutdownPath, false);
      if (stat == null) {
        return false;
      }
      return true;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
