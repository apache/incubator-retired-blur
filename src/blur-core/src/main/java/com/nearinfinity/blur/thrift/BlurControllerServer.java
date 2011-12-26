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

package com.nearinfinity.blur.thrift;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;

import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.BlurPartitioner;
import com.nearinfinity.blur.manager.BlurQueryChecker;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.clusterstatus.ZookeeperPathConstants;
import com.nearinfinity.blur.manager.indexserver.DistributedLayoutManager;
import com.nearinfinity.blur.manager.results.BlurResultIterable;
import com.nearinfinity.blur.manager.results.BlurResultIterableClient;
import com.nearinfinity.blur.manager.results.MergerBlurResultIterable;
import com.nearinfinity.blur.manager.stats.MergerTableStats;
import com.nearinfinity.blur.manager.status.MergerQueryStatus;
import com.nearinfinity.blur.thrift.client.BlurClient;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.TableStats;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.BlurUtil;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.QueryCache;
import com.nearinfinity.blur.utils.QueryCacheEntry;
import com.nearinfinity.blur.utils.QueryCacheKey;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class BlurControllerServer extends TableAdmin implements Iface {

  private static final String CONTROLLER_THREAD_POOL = "controller-thread-pool";
  private static final Log LOG = LogFactory.getLog(BlurControllerServer.class);

  private ExecutorService _executor;
  private AtomicReference<Map<String, Map<String, String>>> _shardServerLayout = new AtomicReference<Map<String, Map<String, String>>>(new HashMap<String, Map<String, String>>());
  private BlurClient _client;
  private int _threadCount = 64;
  private AtomicBoolean _closed = new AtomicBoolean();
  private Map<String, Integer> _tableShardCountMap = new ConcurrentHashMap<String, Integer>();
  private BlurPartitioner<BytesWritable, Void> _blurPartitioner = new BlurPartitioner<BytesWritable, Void>();
  private String _nodeName;
  private int _remoteFetchCount = 100;
  private long _maxTimeToLive = TimeUnit.MINUTES.toMillis(1);
  private int _maxQueryCacheElements = 128;
  private QueryCache _queryCache;
  private BlurQueryChecker _queryChecker;
  private AtomicBoolean _running = new AtomicBoolean();
  private List<Thread> _shardServerWatcherDaemons = new ArrayList<Thread>();

  private int _maxFetchRetries = 1;
  private int _maxMutateRetries = 1;
  private int _maxDefaultRetries = 1;
  private long _fetchDelay = 500;
  private long _mutateDelay = 500;
  private long _defaultDelay = 500;
  private long _maxFetchDelay = 2000;
  private long _maxMutateDelay = 2000;
  private long _maxDefaultDelay = 2000;

  public void init() throws KeeperException, InterruptedException {
    setupZookeeper();
    registerMyself();
    _queryCache = new QueryCache("controller-cache", _maxQueryCacheElements, _maxTimeToLive);
    _executor = Executors.newThreadPool(CONTROLLER_THREAD_POOL, _threadCount);
    _running.set(true);
    List<String> clusterList = _clusterStatus.getClusterList();
    for (String cluster : clusterList) {
      watchForLayoutChanges(cluster);
    }
  }

  private void setupZookeeper() throws KeeperException, InterruptedException {
    BlurUtil.createIfMissing(_zookeeper, "/blur");
    BlurUtil.createIfMissing(_zookeeper, ZookeeperPathConstants.getOnlineControllersPath());
    BlurUtil.createIfMissing(_zookeeper, ZookeeperPathConstants.getClustersPath());
  }

  private void watchForLayoutChanges(final String cluster) {
    Thread shardServerWatcherDaemon = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          synchronized (_shardServerLayout) {
            try {
              String shardNodesPath = ZookeeperPathConstants.getClustersPath() + "/" + cluster + "/online/shard-nodes";
              String tablesPath = ZookeeperPathConstants.getClustersPath() + "/" + cluster + "/tables";
              _zookeeper.getChildren(shardNodesPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                  synchronized (_shardServerLayout) {
                    _shardServerLayout.notifyAll();
                  }
                }
              });
              _zookeeper.getChildren(tablesPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                  synchronized (_shardServerLayout) {
                    _shardServerLayout.notifyAll();
                  }
                }
              });
              LOG.info("Layout changing for cluster [{0}], because of table or shard server change.", cluster);
              updateLayout();
              _shardServerLayout.wait();
            } catch (KeeperException e) {
              if (e.code() == Code.SESSIONEXPIRED) {
                LOG.error("Zookeeper session expired.");
                _running.set(false);
                return;
              }
              LOG.error("Unknown Error", e);
            } catch (InterruptedException e) {
              LOG.error("Unknown Error", e);
            }
          }
        }
      }
    });
    shardServerWatcherDaemon.setName("Shard-Server-Watcher-Daemon-Cluster[" + cluster + "]");
    shardServerWatcherDaemon.setDaemon(true);
    shardServerWatcherDaemon.start();
    _shardServerWatcherDaemons.add(shardServerWatcherDaemon);
  }

  private synchronized void updateLayout() {
    List<String> tableList = _clusterStatus.getTableList();
    HashMap<String, Map<String, String>> newLayout = new HashMap<String, Map<String, String>>();
    for (String table : tableList) {
      DistributedLayoutManager layoutManager = new DistributedLayoutManager();
      String cluster = _clusterStatus.getCluster(false, table);
      List<String> shardServerList = _clusterStatus.getShardServerList(cluster);
      List<String> offlineShardServers = _clusterStatus.getOfflineShardServers(cluster);
      List<String> shardList = getShardList(cluster, table);
      layoutManager.setNodes(shardServerList);
      layoutManager.setNodesOffline(offlineShardServers);
      layoutManager.setShards(shardList);
      layoutManager.init();
      Map<String, String> layout = layoutManager.getLayout();
      newLayout.put(table, layout);
    }
    _shardServerLayout.set(newLayout);
  }

  private List<String> getShardList(String cluster, String table) {
    List<String> shards = new ArrayList<String>();
    TableDescriptor tableDescriptor = _clusterStatus.getTableDescriptor(true, cluster, table);
    for (int i = 0; i < tableDescriptor.shardCount; i++) {
      shards.add(BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, i));
    }
    return shards;
  }

  private void registerMyself() {
    try {
      String onlineControllerPath = ZookeeperPathConstants.getOnlineControllersPath() + "/" + _nodeName;
      while (_zookeeper.exists(onlineControllerPath, false) != null) {
        LOG.info("Node [{0}] already registered, waiting for path [{1}] to be released", _nodeName, onlineControllerPath);
        Thread.sleep(3000);
      }
      String version = BlurUtil.getVersion();
      _zookeeper.create(onlineControllerPath, version.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void close() {
    if (!_closed.get()) {
      _closed.set(true);
      _running.set(false);
      _executor.shutdownNow();
    }
  }

  @Override
  public BlurResults query(final String table, final BlurQuery blurQuery) throws BlurException, TException {
    // @TODO make this faster
    String cluster = _clusterStatus.getCluster(true, table);
    checkTable(cluster, table);
    _queryChecker.checkQuery(blurQuery);
    int shardCount = _clusterStatus.getShardCount(cluster, table);

    OUTER: for (int retries = 0; retries < _maxDefaultRetries; retries++) {
      try {
        final AtomicLongArray facetCounts = BlurUtil.getAtomicLongArraySameLengthAsList(blurQuery.facets);

        BlurQuery original = new BlurQuery(blurQuery);
        if (blurQuery.useCacheIfPresent) {
          LOG.debug("Using cache for query [{0}] on table [{1}].", blurQuery, table);
          QueryCacheKey key = QueryCache.getNormalizedBlurQueryKey(table, blurQuery);
          QueryCacheEntry queryCacheEntry = _queryCache.get(key);
          if (_queryCache.isValid(queryCacheEntry)) {
            LOG.debug("Cache hit for query [{0}] on table [{1}].", blurQuery, table);
            return queryCacheEntry.getBlurResults(blurQuery);
          } else {
            _queryCache.remove(key);
          }
        }

        BlurUtil.setStartTime(original);

        Selector selector = blurQuery.getSelector();
        blurQuery.setSelector(null);

        BlurResultIterable hitsIterable = scatterGather(getCluster(table), new BlurCommand<BlurResultIterable>() {
          @Override
          public BlurResultIterable call(Client client) throws BlurException, TException {
            return new BlurResultIterableClient(client, table, blurQuery, facetCounts, _remoteFetchCount);
          }
        }, new MergerBlurResultIterable(blurQuery));
        BlurResults results = BlurUtil.convertToHits(hitsIterable, blurQuery, facetCounts, _executor, selector, this, table);
        if (!validResults(results, shardCount, blurQuery)) {
          BlurClientManager.sleep(_defaultDelay, _maxDefaultDelay, retries, _maxDefaultRetries);
          continue OUTER;
        }
        return _queryCache.cache(table, original, results);
      } catch (Exception e) {
        LOG.error("Unknown error during search of [table={0},blurQuery={1}]", e, table, blurQuery);
        throw new BException("Unknown error during search of [table={0},blurQuery={1}]", e, table, blurQuery);
      }
    }
    throw new BlurException("Query could not be completed.", null);
  }

  private boolean validResults(BlurResults results, int shardCount, BlurQuery query) {
    if (results.totalResults >= query.minimumNumberOfResults) {
      return true;
    }
    int shardInfoSize = results.getShardInfoSize();
    if (shardInfoSize == shardCount) {
      return true;
    }
    return false;
  }

  @Override
  public FetchResult fetchRow(final String table, final Selector selector) throws BlurException, TException {
    String cluster = _clusterStatus.getCluster(true, table);
    checkTable(cluster, table);
    IndexManager.validSelector(selector);
    String clientHostnamePort = null;
    try {
      clientHostnamePort = getNode(table, selector);
      return _client.execute(clientHostnamePort, new BlurCommand<FetchResult>() {
        @Override
        public FetchResult call(Client client) throws BlurException, TException {
          return client.fetchRow(table, selector);
        }
      }, _maxFetchRetries, _fetchDelay, _maxFetchDelay);
    } catch (Exception e) {
      LOG.error("Unknown error during fetch of row from table [{0}] selector [{1}] node [{2}]", e, table, selector, clientHostnamePort);
      throw new BException("Unknown error during fetch of row from table [{0}] selector [{1}] node [{2}]", e, table, selector, clientHostnamePort);
    }
  }

  @Override
  public void cancelQuery(final String table, final long uuid) throws BlurException, TException {
    String cluster = _clusterStatus.getCluster(true, table);
    checkTable(cluster, table);
    try {
      scatter(getCluster(table), new BlurCommand<Void>() {
        @Override
        public Void call(Client client) throws BlurException, TException {
          client.cancelQuery(table, uuid);
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to cancel search table [{0}] uuid [{1}]", e, table, uuid);
      throw new BException("Unknown error while trying to cancel search table [{0}] uuid [{1}]", e, table, uuid);
    }
  }

  @Override
  public List<BlurQueryStatus> currentQueries(final String table) throws BlurException, TException {
    String cluster = _clusterStatus.getCluster(true, table);
    checkTable(cluster, table);
    try {
      return scatterGather(getCluster(table), new BlurCommand<List<BlurQueryStatus>>() {
        @Override
        public List<BlurQueryStatus> call(Client client) throws BlurException, TException {
          return client.currentQueries(table);
        }
      }, new MergerQueryStatus());
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get current searches [{0}]", e, table);
      throw new BException("Unknown error while trying to get current searches [{0}]", e, table);
    }
  }

  @Override
  public TableStats getTableStats(final String table) throws BlurException, TException {
    String cluster = _clusterStatus.getCluster(true, table);
    checkTable(cluster, table);
    try {
      return scatterGather(getCluster(table), new BlurCommand<TableStats>() {
        @Override
        public TableStats call(Client client) throws BlurException, TException {
          return client.getTableStats(table);
        }
      }, new MergerTableStats());
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get table stats [{0}]", e, table);
      throw new BException("Unknown error while trying to get table stats [{0}]", e, table);
    }
  }

  @Override
  public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
    String cluster = _clusterStatus.getCluster(true, table);
    checkTable(cluster, table);
    Map<String, Map<String, String>> layout = _shardServerLayout.get();
    Map<String, String> tableLayout = layout.get(table);
    if (tableLayout == null) {
      return new HashMap<String, String>();
    }
    return tableLayout;
  }

  @Override
  public long recordFrequency(final String table, final String columnFamily, final String columnName, final String value) throws BlurException, TException {
    String cluster = _clusterStatus.getCluster(true, table);
    checkTable(cluster, table);
    try {
      return scatterGather(getCluster(table), new BlurCommand<Long>() {
        @Override
        public Long call(Client client) throws BlurException, TException {
          return client.recordFrequency(table, columnFamily, columnName, value);
        }
      }, new Merger<Long>() {
        @Override
        public Long merge(BlurExecutorCompletionService<Long> service) throws Exception {
          Long total = 0L;
          while (service.getRemainingCount() > 0) {
            total += service.take().get();
          }
          return total;
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get record frequency [{0}/{1}/{2}/{3}]", e, table, columnFamily, columnName, value);
      throw new BException("Unknown error while trying to get record frequency [{0}/{1}/{2}/{3}]", e, table, columnFamily, columnName, value);
    }
  }

  @Override
  public Schema schema(final String table) throws BlurException, TException {
    String cluster = _clusterStatus.getCluster(true, table);
    checkTable(cluster, table);
    try {
      return scatterGather(getCluster(table), new BlurCommand<Schema>() {
        @Override
        public Schema call(Client client) throws BlurException, TException {
          return client.schema(table);
        }
      }, new Merger<Schema>() {
        @Override
        public Schema merge(BlurExecutorCompletionService<Schema> service) throws Exception {
          Schema result = null;
          while (service.getRemainingCount() > 0) {
            Schema schema = service.take().get();
            if (result == null) {
              result = schema;
            } else {
              result = BlurControllerServer.merge(result, schema);
            }
          }
          return result;
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to schema table [{0}]", e, table);
      throw new BException("Unknown error while trying to schema table [{0}]", e, table);
    }
  }

  @Override
  public List<String> terms(final String table, final String columnFamily, final String columnName, final String startWith, final short size) throws BlurException, TException {
    String cluster = _clusterStatus.getCluster(true, table);
    checkTable(cluster, table);
    try {
      return scatterGather(getCluster(table), new BlurCommand<List<String>>() {
        @Override
        public List<String> call(Client client) throws BlurException, TException {
          return client.terms(table, columnFamily, columnName, startWith, size);
        }
      }, new Merger<List<String>>() {
        @Override
        public List<String> merge(BlurExecutorCompletionService<List<String>> service) throws Exception {
          TreeSet<String> terms = new TreeSet<String>();
          while (service.getRemainingCount() > 0) {
            terms.addAll(service.take().get());
          }
          return new ArrayList<String>(terms).subList(0, Math.min(terms.size(), size));
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to terms table [{0}] columnFamily [{1}] columnName [{2}] startWith [{3}] size [{4}]", e, table, columnFamily, columnName, startWith,
          size);
      throw new BException("Unknown error while trying to terms table [{0}] columnFamily [{1}] columnName [{2}] startWith [{3}] size [{4}]", e, table, columnFamily, columnName,
          startWith, size);
    }
  }

  private String getNode(String table, Selector selector) throws BlurException, TException {
    Map<String, String> layout = shardServerLayout(table);
    String locationId = selector.locationId;
    if (locationId != null) {
      String shard = locationId.substring(0, locationId.indexOf('/'));
      return layout.get(shard);
    }
    int numberOfShards = getShardCount(table);
    if (selector.rowId != null) {
      String shardName = MutationHelper.getShardName(table, selector.rowId, numberOfShards, _blurPartitioner);
      return layout.get(shardName);
    }
    throw new BlurException("Selector is missing both a locationid and a rowid, one is needed.", null);
  }

  private <R> R scatterGather(String cluster, final BlurCommand<R> command, Merger<R> merger) throws Exception {
    return ForkJoin.execute(_executor, _clusterStatus.getOnlineShardServers(cluster), new ParallelCall<String, R>() {
      @SuppressWarnings("unchecked")
      @Override
      public R call(String hostnamePort) throws Exception {
        return _client.execute(hostnamePort, (BlurCommand<R>) command.clone(), _maxDefaultRetries, _defaultDelay, _maxDefaultDelay);
      }
    }).merge(merger);
  }

  private <R> void scatter(String cluster, BlurCommand<R> command) throws Exception {
    scatterGather(cluster, command, new Merger<R>() {
      @Override
      public R merge(BlurExecutorCompletionService<R> service) throws Exception {
        while (service.getRemainingCount() > 0) {
          service.take().get();
        }
        return null;
      }
    });
  }

  private String getCluster(String table) throws BlurException, TException {
    TableDescriptor describe = describe(table);
    if (describe == null) {
      throw new BlurException("Table [" + table + "] not found.", null);
    }
    return describe.cluster;
  }

  public static Schema merge(Schema result, Schema schema) {
    Map<String, Set<String>> destColumnFamilies = result.columnFamilies;
    Map<String, Set<String>> srcColumnFamilies = schema.columnFamilies;
    for (String srcColumnFamily : srcColumnFamilies.keySet()) {
      Set<String> destColumnNames = destColumnFamilies.get(srcColumnFamily);
      Set<String> srcColumnNames = srcColumnFamilies.get(srcColumnFamily);
      if (destColumnNames == null) {
        destColumnFamilies.put(srcColumnFamily, srcColumnNames);
      } else {
        destColumnNames.addAll(srcColumnNames);
      }
    }
    return result;
  }

  @Override
  public void mutate(final RowMutation mutation) throws BlurException, TException {
    String cluster = _clusterStatus.getCluster(true, mutation.table);
    checkForUpdates(cluster, mutation.table);
    checkTable(cluster, mutation.table);
    try {
      MutationHelper.validateMutation(mutation);
      String table = mutation.getTable();

      int numberOfShards = getShardCount(table);
      Map<String, String> tableLayout = _shardServerLayout.get().get(table);
      if (tableLayout.size() != numberOfShards) {
        throw new BlurException("Cannot update data while shard is missing", null);
      }

      String shardName = MutationHelper.getShardName(table, mutation.rowId, numberOfShards, _blurPartitioner);
      String node = tableLayout.get(shardName);
      _client.execute(node, new BlurCommand<Void>() {
        @Override
        public Void call(Client client) throws BlurException, TException {
          client.mutate(mutation);
          return null;
        }
      }, _maxMutateRetries, _mutateDelay, _maxMutateDelay);
    } catch (Exception e) {
      LOG.error("Unknown error during mutation of [{0}]", e, mutation);
      throw new BException("Unknown error during mutation of [{0}]", e, mutation);
    }
  }

  private int getShardCount(String table) throws BlurException, TException {
    Integer numberOfShards = _tableShardCountMap.get(table);
    if (numberOfShards == null) {
      TableDescriptor descriptor = describe(table);
      numberOfShards = descriptor.shardCount;
      _tableShardCountMap.put(table, numberOfShards);
    }
    return numberOfShards;
  }

  @Override
  public void mutateBatch(List<RowMutation> mutations) throws BlurException, TException {
    for (RowMutation mutation : mutations) {
      MutationHelper.validateMutation(mutation);
    }
    for (RowMutation mutation : mutations) {
      mutate(mutation);
    }
  }

  public void setNodeName(String nodeName) {
    _nodeName = nodeName;
  }

  public int getRemoteFetchCount() {
    return _remoteFetchCount;
  }

  public void setRemoteFetchCount(int remoteFetchCount) {
    _remoteFetchCount = remoteFetchCount;
  }

  public long getMaxTimeToLive() {
    return _maxTimeToLive;
  }

  public void setMaxTimeToLive(long maxTimeToLive) {
    _maxTimeToLive = maxTimeToLive;
  }

  public int getMaxQueryCacheElements() {
    return _maxQueryCacheElements;
  }

  public void setMaxQueryCacheElements(int maxQueryCacheElements) {
    _maxQueryCacheElements = maxQueryCacheElements;
  }

  public void setQueryChecker(BlurQueryChecker queryChecker) {
    _queryChecker = queryChecker;
  }

  public void setThreadCount(int threadCount) {
    _threadCount = threadCount;
  }

  public void setMaxFetchRetries(int maxFetchRetries) {
    _maxFetchRetries = maxFetchRetries;
  }

  public void setMaxMutateRetries(int maxMutateRetries) {
    _maxMutateRetries = maxMutateRetries;
  }

  public void setMaxDefaultRetries(int maxDefaultRetries) {
    _maxDefaultRetries = maxDefaultRetries;
  }

  public void setFetchDelay(long fetchDelay) {
    _fetchDelay = fetchDelay;
  }

  public void setMutateDelay(long mutateDelay) {
    _mutateDelay = mutateDelay;
  }

  public void setDefaultDelay(long defaultDelay) {
    _defaultDelay = defaultDelay;
  }

  public void setMaxFetchDelay(long maxFetchDelay) {
    _maxFetchDelay = maxFetchDelay;
  }

  public void setMaxMutateDelay(long maxMutateDelay) {
    _maxMutateDelay = maxMutateDelay;
  }

  public void setMaxDefaultDelay(long maxDefaultDelay) {
    _maxDefaultDelay = maxDefaultDelay;
  }

  public BlurClient getClient() {
    return _client;
  }

  public void setClient(BlurClient client) {
    _client = client;
  }
}