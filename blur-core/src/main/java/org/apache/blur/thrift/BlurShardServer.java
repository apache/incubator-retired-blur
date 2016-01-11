package org.apache.blur.thrift;

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
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_CACHE_MAX_QUERYCACHE_ELEMENTS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_CACHE_MAX_TIMETOLIVE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_DATA_FETCH_THREAD_COUNT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.blur.command.ArgumentOverlay;
import org.apache.blur.command.BlurObject;
import org.apache.blur.command.BlurObjectSerDe;
import org.apache.blur.command.CommandUtil;
import org.apache.blur.command.Response;
import org.apache.blur.command.ShardCommandManager;
import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.BlurQueryChecker;
import org.apache.blur.manager.IndexManager;
import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.results.BlurResultIterable;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.server.ShardServerContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.server.TableContextFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Arguments;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.CommandDescriptor;
import org.apache.blur.thrift.generated.CommandStatus;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.HighlightOptions;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.Status;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.thrift.generated.TimeoutException;
import org.apache.blur.thrift.generated.User;
import org.apache.blur.user.UserContext;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.QueryCache;
import org.apache.blur.utils.QueryCacheEntry;
import org.apache.blur.utils.QueryCacheKey;

public class BlurShardServer extends TableAdmin implements Iface {

  private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
  private static final boolean ENABLE_CACHE = false;
  private IndexManager _indexManager;
  private IndexServer _indexServer;
  private boolean _closed;
  private long _maxTimeToLive = TimeUnit.MINUTES.toMillis(1);
  private int _maxQueryCacheElements = 128;
  private QueryCache _queryCache;
  private BlurQueryChecker _queryChecker;
  private ExecutorService _dataFetch;
  private String _cluster = BlurConstants.BLUR_CLUSTER;
  private int _dataFetchThreadCount = 32;
  private ShardCommandManager _commandManager;
  private BlurObjectSerDe _serDe = new BlurObjectSerDe();

  public void init() throws BlurException {
    _queryCache = new QueryCache("shard-cache", _maxQueryCacheElements, _maxTimeToLive);
    _dataFetch = Executors.newThreadPool("data-fetch-", _dataFetchThreadCount);

    if (_configuration == null) {
      throw new BException("Configuration must be set before initialization.");
    }
    _cluster = _configuration.get(BlurConstants.BLUR_CLUSTER_NAME, BlurConstants.BLUR_CLUSTER);
    _dataFetchThreadCount = _configuration.getInt(BLUR_SHARD_DATA_FETCH_THREAD_COUNT, 8);
    _maxQueryCacheElements = _configuration.getInt(BLUR_SHARD_CACHE_MAX_QUERYCACHE_ELEMENTS, 128);
    _maxTimeToLive = _configuration.getLong(BLUR_SHARD_CACHE_MAX_TIMETOLIVE, TimeUnit.MINUTES.toMillis(1));
  }

  @Override
  public BlurResults query(String table, BlurQuery blurQuery) throws BlurException, TException {
    try {
      checkTable(_cluster, table);
      resetSearchers();
      _queryChecker.checkQuery(blurQuery);
      checkSelectorFetchSize(blurQuery.getSelector());
      BlurQuery original = new BlurQuery(blurQuery);
      Selector selector = original.getSelector();
      if (selector != null) {
        HighlightOptions highlightOptions = selector.getHighlightOptions();
        if (highlightOptions != null && highlightOptions.getQuery() == null) {
          highlightOptions.setQuery(blurQuery.getQuery());
        }
      }

      // Note: Querying the Shard Server directly if query.startTime == 0
      BlurUtil.setStartTime(blurQuery);
      if (ENABLE_CACHE) {
        if (blurQuery.useCacheIfPresent && selector == null) {
          // Selector has to be null because we might cache data if it's not.
          LOG.debug("Using cache for query [{0}] on table [{1}].", blurQuery, table);
          QueryCacheKey key = QueryCache.getNormalizedBlurQueryKey(table, blurQuery);
          QueryCacheEntry queryCacheEntry = _queryCache.get(key);
          if (_queryCache.isValid(queryCacheEntry, _indexServer.getShardListCurrentServerOnly(table))) {
            LOG.debug("Cache hit for query [{0}] on table [{1}].", blurQuery, table);
            return queryCacheEntry.getBlurResults(blurQuery);
          } else {
            _queryCache.remove(key);
          }
        }
      }
      BlurUtil.setStartTime(original);
      BlurResultIterable hitsIterable = null;
      try {
        AtomicLongArray facetCounts = BlurUtil.getAtomicLongArraySameLengthAsList(blurQuery.facets);
        hitsIterable = _indexManager.query(table, blurQuery, facetCounts);
        // Data will be fetch by IndexManager if selector is provided.
        // This should only happen if the Shard server is accessed directly.
        BlurResults blurResults = BlurUtil.convertToHits(hitsIterable, blurQuery, facetCounts, null, null, this, table);
        if (selector != null) {
          return blurResults;
        }
        if (ENABLE_CACHE) {
          return _queryCache.cache(table, original, blurResults);
        }
        return blurResults;
      } catch (BlurException e) {
        throw e;
      } catch (Exception e) {
        LOG.error("Unknown error during search of [table={0},searchQuery={1}]", e, table, blurQuery);
        throw new BException(e.getMessage(), e);
      } finally {
        if (hitsIterable != null) {
          hitsIterable.close();
        }
      }
    } catch (Exception e) {
      LOG.error("Unknown error during search of [table={0},searchQuery={1}]", e, table, blurQuery);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public String parseQuery(String table, Query simpleQuery) throws BlurException, TException {
    try {
      checkTable(_cluster, table);
      resetSearchers();
      return _indexManager.parseQuery(table, simpleQuery);
    } catch (Exception e) {
      LOG.error("Unknown error during parsing of [table={0},simpleQuery={1}]", e, table, simpleQuery);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
    try {
      checkTable(_cluster, table);
      checkSelectorFetchSize(selector);
      FetchResult fetchResult = new FetchResult();
      _indexManager.fetchRow(table, selector, fetchResult);
      return fetchResult;
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get fetch row [table={0},selector={1}]", e, table, selector);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public List<FetchResult> fetchRowBatch(String table, List<Selector> selectors) throws BlurException, TException {
    try {
      checkTable(_cluster, table);
      for (Selector selector : selectors) {
        checkSelectorFetchSize(selector);
      }
      return _indexManager.fetchRowBatch(table, selectors);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get fetch row [table={0},selector={1}]", e, table, selectors);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void cancelQuery(String table, String uuid) throws BlurException, TException {
    try {
      checkTable(_cluster, table);
      resetSearchers();
      _indexManager.cancelQuery(table, uuid);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to cancel search [uuid={0}]", e, uuid);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  private void resetSearchers() {
    ShardServerContext.resetSearchers();
  }

  @Override
  public TableStats tableStats(String table) throws BlurException, TException {
    try {
      checkTable(_cluster, table);
      resetSearchers();
      TableStats tableStats = new TableStats();
      tableStats.tableName = table;
      tableStats.recordCount = _indexServer.getRecordCount(table);
      tableStats.rowCount = _indexServer.getRowCount(table);
      tableStats.bytes = _indexServer.getTableSize(table);
      tableStats.segmentImportInProgressCount = _indexServer.getSegmentImportInProgressCount(table);
      tableStats.segmentImportPendingCount = _indexServer.getSegmentImportPendingCount(table);
      return tableStats;
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get table stats [table={0}]", e, table);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  public synchronized void close() {
    if (!_closed) {
      _closed = true;
      _indexManager.close();
      _dataFetch.shutdownNow();
    }
  }

  @Override
  public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
    try {
      checkTable(_cluster, table);
      resetSearchers();
      Map<String, BlurIndex> blurIndexes = _indexServer.getIndexes(table);
      Map<String, String> result = new TreeMap<String, String>();
      String nodeName = _indexServer.getNodeName();
      for (String shard : blurIndexes.keySet()) {
        result.put(shard, nodeName);
      }
      return result;
    } catch (Exception e) {
      LOG.error("Unknown error while trying to getting shardServerLayout for table [" + table + "]", e);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public Map<String, Map<String, ShardState>> shardServerLayoutState(String table) throws BlurException, TException {
    try {
      resetSearchers();
      Map<String, Map<String, ShardState>> result = new TreeMap<String, Map<String, ShardState>>();
      String nodeName = _indexServer.getNodeName();
      Map<String, ShardState> stateMap = _indexServer.getShardState(table);
      for (Entry<String, ShardState> entry : stateMap.entrySet()) {
        result.put(entry.getKey(), newMap(nodeName, entry.getValue()));
      }
      return result;
    } catch (Exception e) {
      LOG.error("Unknown error while trying to getting shardServerLayoutState for table [" + table + "]", e);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  private Map<String, ShardState> newMap(String nodeName, ShardState state) {
    Map<String, ShardState> map = new HashMap<String, ShardState>();
    map.put(nodeName, state);
    return map;
  }

  @Override
  public long recordFrequency(String table, String columnFamily, String columnName, String value) throws BlurException,
      TException {
    try {
      checkTable(_cluster, table);
      resetSearchers();
      return _indexManager.recordFrequency(table, columnFamily, columnName, value);
    } catch (Exception e) {
      LOG.error(
          "Unknown error while trying to get record frequency for [table={0},columnFamily={1},columnName={2},value={3}]",
          e, table, columnFamily, columnName, value);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public Schema schema(String table) throws BlurException, TException {
    try {
      resetSearchers();
      return super.schema(table);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get schema for [table={0}]", e, table);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public List<String> terms(String table, String columnFamily, String columnName, String startWith, short size)
      throws BlurException, TException {
    try {
      checkTable(_cluster, table);
      resetSearchers();
      return _indexManager.terms(table, columnFamily, columnName, startWith, size);
    } catch (Exception e) {
      LOG.error(
          "Unknown error while trying to get terms list for [table={0},columnFamily={1},columnName={2},startWith={3},size={4}]",
          e, table, columnFamily, columnName, startWith, size);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void mutate(RowMutation mutation) throws BlurException, TException {
    try {
      checkTable(_cluster, mutation.table);
      checkForUpdates(_cluster, mutation.table);
      resetSearchers();
      MutationHelper.validateMutation(mutation);
      _indexManager.mutate(mutation);
    } catch (Exception e) {
      LOG.error("Unknown error during processing of [mutation={0}]", e, mutation);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void enqueueMutate(RowMutation mutation) throws BlurException, TException {
    try {
      checkTable(_cluster, mutation.table);
      checkForUpdates(_cluster, mutation.table);
      resetSearchers();
      MutationHelper.validateMutation(mutation);
      _indexManager.enqueue(mutation);
    } catch (Exception e) {
      LOG.error("Unknown error during processing of [mutation={0}]", e, mutation);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void mutateBatch(List<RowMutation> mutations) throws BlurException, TException {
    try {
      resetSearchers();
      for (RowMutation mutation : mutations) {
        checkTable(_cluster, mutation.table);
        checkForUpdates(_cluster, mutation.table);
        MutationHelper.validateMutation(mutation);
      }
      _indexManager.mutate(mutations);
    } catch (Exception e) {
      LOG.error("Unknown error during processing of [mutations={0}]", e, mutations);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void enqueueMutateBatch(List<RowMutation> mutations) throws BlurException, TException {
    try {
      resetSearchers();
      for (RowMutation mutation : mutations) {
        checkTable(_cluster, mutation.table);
        checkForUpdates(_cluster, mutation.table);
        MutationHelper.validateMutation(mutation);
      }
      _indexManager.enqueue(mutations);
    } catch (Exception e) {
      LOG.error("Unknown error during processing of [mutations={0}]", e, mutations);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
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

  public void setIndexManager(IndexManager indexManager) {
    _indexManager = indexManager;
  }

  public void setIndexServer(IndexServer indexServer) {
    _indexServer = indexServer;
  }

  @Override
  public BlurQueryStatus queryStatusById(String table, String uuid) throws BlurException, TException {
    try {
      checkTable(_cluster, table);
      resetSearchers();
      BlurQueryStatus blurQueryStatus;
      blurQueryStatus = _indexManager.queryStatus(table, uuid);
      if (blurQueryStatus == null) {
        blurQueryStatus = new BlurQueryStatus();
        blurQueryStatus.status = Status.NOT_FOUND;
      } else {
        blurQueryStatus.status = Status.FOUND;
      }
      return blurQueryStatus;
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get current query status [table={0},uuid={1}]", e, table, uuid);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public List<String> queryStatusIdList(String table) throws BlurException, TException {
    checkTable(_cluster, table);
    resetSearchers();
    try {
      return _indexManager.queryStatusIdList(table);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get query status id list [table={0}]", e, table);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void optimize(String table, int numberOfSegmentsPerShard) throws BlurException, TException {
    try {
      checkTable(_cluster, table);
      resetSearchers();
      _indexManager.optimize(table, numberOfSegmentsPerShard);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to optimize [table={0},numberOfSegmentsPerShard={1}]", e, table,
          numberOfSegmentsPerShard);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void createSnapshot(final String table, final String name) throws BlurException, TException {
    try {
      checkTable(_cluster, table);
      Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);
      for (Entry<String, BlurIndex> entry : indexes.entrySet()) {
        BlurIndex index = entry.getValue();
        index.createSnapshot(name);
      }
    } catch (Exception e) {
      LOG.error("Unknown error while trying to getting indexes for table [" + table + "]", e);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void removeSnapshot(final String table, final String name) throws BlurException, TException {
    try {
      Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);
      for (Entry<String, BlurIndex> entry : indexes.entrySet()) {
        BlurIndex index = entry.getValue();
        index.removeSnapshot(name);
      }
    } catch (Exception e) {
      LOG.error("Unknown error while trying to getting indexes for table [" + table + "]", e);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public Map<String, List<String>> listSnapshots(final String table) throws BlurException, TException {
    try {
      Map<String, List<String>> snapshots = new HashMap<String, List<String>>();
      Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);
      for (Entry<String, BlurIndex> entry : indexes.entrySet()) {
        BlurIndex index = entry.getValue();
        List<String> shardSnapshots = index.getSnapshots();
        if (shardSnapshots == null) {
          shardSnapshots = new ArrayList<String>();
        }
        snapshots.put(entry.getKey(), shardSnapshots);
      }
      return snapshots;
    } catch (Exception e) {
      LOG.error("Unknown error while trying to getting indexes for table [" + table + "]", e);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  public int getDataFetchThreadCount() {
    return _dataFetchThreadCount;
  }

  public void setDataFetchThreadCount(int dataFetchThreadCount) {
    _dataFetchThreadCount = dataFetchThreadCount;
  }

  @Override
  public void setUser(User user) throws TException {
    ShardServerContext context = ShardServerContext.getShardServerContext();
    context.setUser(user);
  }

  @Override
  public void startTrace(String rootId, String requestId) throws TException {
    ShardServerContext context = ShardServerContext.getShardServerContext();
    context.setTraceRootId(rootId);
    context.setTraceRequestId(requestId);
  }

  protected boolean inSafeMode(boolean useCache, String table) throws BlurException {
    // Shard server cannot be processing requests if it's in safe mode.
    return false;
  }

  @Override
  public org.apache.blur.thrift.generated.Response execute(String commandName, Arguments arguments)
      throws BlurException, TException {
    try {
      TableContextFactory tableContextFactory = new TableContextFactory() {
        @Override
        public TableContext getTableContext(String table) throws IOException {
          return TableContext.create(_clusterStatus.getTableDescriptor(true, _clusterStatus.getCluster(true, table),
              table));
        }
      };
      BlurObject args = CommandUtil.toBlurObject(arguments);
      User thriftUser = UserConverter.toThriftUser(UserContext.getUser());
      CommandStatus originalCommandStatusObject = new CommandStatus(null, commandName, arguments, null, thriftUser);
      Response response = _commandManager.execute(tableContextFactory, commandName, new ArgumentOverlay(args, _serDe),
          originalCommandStatusObject);
      return CommandUtil.fromObjectToThrift(response, _serDe);
    } catch (Exception e) {
      if (e instanceof org.apache.blur.command.TimeoutException) {
        throw new TimeoutException(((org.apache.blur.command.TimeoutException) e).getInstanceExecutionId());
      }
      LOG.error("Unknown error while trying to execute command [{0}] for table [{1}]", e, commandName);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  public void setCommandManager(ShardCommandManager commandManager) {
    _commandManager = commandManager;
  }

  @Override
  public org.apache.blur.thrift.generated.Response reconnect(long instanceExecutionId) throws BlurException,
      TimeoutException, TException {
    try {
      Response response = _commandManager.reconnect(instanceExecutionId);
      return CommandUtil.fromObjectToThrift(response, _serDe);
    } catch (Exception e) {
      if (e instanceof org.apache.blur.command.TimeoutException) {
        throw new TimeoutException(((org.apache.blur.command.TimeoutException) e).getInstanceExecutionId());
      }
      LOG.error("Unknown error while trying to reconnect to executing command [{0}]", e, instanceExecutionId);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void refresh() throws TException {
    ShardServerContext.resetSearchers();
  }

  @Override
  public List<CommandDescriptor> listInstalledCommands() throws BlurException, TException {
    try {
      return listInstalledCommands(_commandManager);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get a list of installed commands [{0}]", e);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public List<String> commandStatusList(int startingAt, short fetch) throws BlurException, TException {
    try {
      List<String> ids = _commandManager.commandStatusList();
      return ids.subList(startingAt, Math.min(ids.size(), fetch));
    } catch (Exception e) {
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public CommandStatus commandStatus(String commandExecutionId) throws BlurException, TException {
    try {
      return _commandManager.getCommandStatus(commandExecutionId);
    } catch (Exception e) {
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void commandCancel(String commandExecutionId) throws BlurException, TException {
    try {
      _commandManager.cancelCommand(commandExecutionId);
    } catch (Exception e) {
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void bulkMutateAdd(String bulkId, RowMutation rowMutation) throws BlurException, TException {
    String table = rowMutation.getTable();
    checkTable(table);
    checkForUpdates(table);
    MutationHelper.validateMutation(rowMutation);
    try {
      _indexManager.bulkMutateAdd(table, bulkId, rowMutation);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to add to a bulk mutate on table [" + table + "]", e);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void bulkMutateFinish(String bulkId, boolean apply, boolean blockUntilComplete) throws BlurException,
      TException {
    try {
      List<String> tableListByCluster = tableListByCluster(_cluster);
      List<String> writableTables = new ArrayList<String>();
      for (String table : tableListByCluster) {
        if (!_clusterStatus.isReadOnly(true, _cluster, table)) {
          writableTables.add(table);
        }
      }
      _indexManager.bulkMutateFinish(new HashSet<String>(writableTables), bulkId, apply, blockUntilComplete);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to finsh a bulk mutate [" + bulkId + "]", e);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void bulkMutateAddMultiple(String bulkId, List<RowMutation> rowMutations) throws BlurException, TException {
    Map<String, List<RowMutation>> batches = batchByTable(rowMutations);
    for (Entry<String, List<RowMutation>> entry : batches.entrySet()) {
      String table = entry.getKey();
      List<RowMutation> batch = entry.getValue();
      try {
        _indexManager.bulkMutateAddMultiple(table, bulkId, batch);
      } catch (Exception e) {
        LOG.error("Unknown error while trying to add to a bulk mutate on table [" + table + "]", e);
        if (e instanceof BlurException) {
          throw (BlurException) e;
        }
        throw new BException(e.getMessage(), e);
      }
    }
  }

  private Map<String, List<RowMutation>> batchByTable(List<RowMutation> rowMutations) throws BlurException {
    Map<String, List<RowMutation>> result = new HashMap<String, List<RowMutation>>();
    for (RowMutation rowMutation : rowMutations) {
      String table = rowMutation.getTable();
      checkTable(table);
      checkForUpdates(table);
      List<RowMutation> list = result.get(table);
      if (list == null) {
        result.put(table, list = new ArrayList<RowMutation>());
      }
      MutationHelper.validateMutation(rowMutation);
      list.add(rowMutation);
    }
    return result;
  }

  @Override
  public void loadData(String table, String location) throws BlurException, TException {
    throw new RuntimeException("Shard servers do not support this call.");
  }

}
