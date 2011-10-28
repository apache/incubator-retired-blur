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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.thrift.TException;

import com.nearinfinity.blur.concurrent.ExecutionContext;
import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.concurrent.ThreadWatcher;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.BlurQueryChecker;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.results.BlurResultIterable;
import com.nearinfinity.blur.manager.writer.BlurIndex;
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
import com.nearinfinity.blur.utils.BlurUtil;
import com.nearinfinity.blur.utils.QueryCache;
import com.nearinfinity.blur.utils.QueryCacheEntry;

public class BlurShardServer extends ExecutionContextIface {

  private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
  private IndexManager _indexManager;
  private IndexServer _indexServer;
  private boolean _closed;
  private long _maxTimeToLive = TimeUnit.MINUTES.toMillis(1);
  private int _maxQueryCacheElements = 128;
  private QueryCache _queryCache;
  private BlurQueryChecker _queryChecker;
  private ExecutorService _dataFetch;
  private ThreadWatcher _threadWatcher;

  public void init() {
    super.init();
    _queryCache = new QueryCache("shard-cache", _maxQueryCacheElements, _maxTimeToLive);
    _dataFetch = Executors.newThreadPool(_threadWatcher, "data-fetch-", 32);
  }

  public enum Metrics {
    GET_TABLE_STATUS, QUERY, FETCH_ROW, SHARD_SERVER_LAYOUT, CANCEL_QUERY, CURRENT_QUERIES, RECORD_FREQUENCY, SCHEMA, TERMS, DESCRIBE, MUTATE, GET_TABLE_STATS
  }

  @Override
  public BlurResults query(ExecutionContext context, String table, BlurQuery blurQuery) throws BlurException, TException {
    checkTable(table);
    _queryChecker.checkQuery(blurQuery);
    long start = context.startTime();
    try {
      checkTableStatus(context, table);
      BlurQuery original = new BlurQuery(blurQuery);
      if (blurQuery.useCacheIfPresent) {
        LOG.debug("Using cache for query [{0}] on table [{1}].", blurQuery, table);
        BlurQuery noralizedBlurQuery = _queryCache.getNormalizedBlurQuery(blurQuery);
        QueryCacheEntry queryCacheEntry = _queryCache.get(noralizedBlurQuery);
        if (_queryCache.isValid(queryCacheEntry)) {
          LOG.debug("Cache hit for query [{0}] on table [{1}].", blurQuery, table);
          return queryCacheEntry.getBlurResults(blurQuery);
        }
      }
      BlurUtil.setStartTime(original);
      try {
        AtomicLongArray facetCounts = BlurUtil.getAtomicLongArraySameLengthAsList(blurQuery.facets);
        BlurResultIterable hitsIterable = _indexManager.query(table, blurQuery, facetCounts);
        return _queryCache.cache(original, BlurUtil.convertToHits(hitsIterable, blurQuery, facetCounts, _dataFetch, blurQuery.selector, this, table));
      } catch (Exception e) {
        LOG.error("Unknown error during search of [table={0},searchQuery={1}]", e, table, blurQuery);
        throw new BException(e.getMessage(), e);
      }
    } finally {
      context.recordTime(Metrics.QUERY, start, table, blurQuery);
    }
  }

  @Override
  public FetchResult fetchRow(ExecutionContext context, String table, Selector selector) throws BlurException, TException {
    checkTable(table);
    long start = context.startTime();
    try {
      checkTableStatus(context, table);
      try {
        FetchResult fetchResult = new FetchResult();
        _indexManager.fetchRow(table, selector, fetchResult);
        return fetchResult;
      } catch (Exception e) {
        LOG.error("Unknown error while trying to get fetch row [table={0},selector={1}]", e, table, selector);
        throw new BException(e.getMessage(), e);
      }
    } finally {
      context.recordTime(Metrics.FETCH_ROW, start, table, selector);
    }
  }

  @Override
  public void cancelQuery(ExecutionContext context, String table, long uuid) throws BlurException, TException {
    checkTable(table);
    long start = context.startTime();
    try {
      _indexManager.cancelQuery(table, uuid);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to cancel search [uuid={0}]", e, uuid);
      throw new BException(e.getMessage(), e);
    } finally {
      context.recordTime(Metrics.CANCEL_QUERY, start, table, uuid);
    }
  }

  @Override
  public List<BlurQueryStatus> currentQueries(ExecutionContext context, String table) throws BlurException, TException {
    checkTable(table);
    long start = context.startTime();
    try {
      checkTableStatus(context, table);
      try {
        return _indexManager.currentQueries(table);
      } catch (Exception e) {
        LOG.error("Unknown error while trying to get current search status [table={0}]", e, table);
        throw new BException(e.getMessage(), e);
      }
    } finally {
      context.recordTime(Metrics.CURRENT_QUERIES, start, table);
    }
  }

  @Override
  public TableStats getTableStats(ExecutionContext context, String table) throws BlurException, TException {
    long start = context.startTime();
    try {
      checkTableStatus(context, table);
      try {
        TableStats tableStats = new TableStats();
        tableStats.tableName = table;
        tableStats.recordCount = _indexServer.getRecordCount(table);
        tableStats.rowCount = _indexServer.getRowCount(table);
        tableStats.bytes = _indexServer.getTableSize(table);
        tableStats.queries = 0;
        return tableStats;
      } catch (Exception e) {
        LOG.error("Unknown error while trying to get table stats [table={0}]", e, table);
        throw new BException(e.getMessage(), e);
      }
    } finally {
      context.recordTime(Metrics.GET_TABLE_STATS, start, table);
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
  public Map<String, String> shardServerLayout(ExecutionContext context, String table) throws BlurException, TException {
    checkTable(table);
    long start = context.startTime();
    try {
      checkTableStatus(context, table);
      try {
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
    } finally {
      context.recordTime(Metrics.SHARD_SERVER_LAYOUT, start, table);
    }
  }

  private void checkTableStatus(ExecutionContext context, String table) throws BlurException, TException {
    if (!isTableEnabled(table)) {
      List<String> tableList = tableList();
      if (tableList.contains(table)) {
        throw new BlurException("Table [" + table + "] is disabled.", null);
      } else {
        throw new BlurException("Table [" + table + "] does not exist.", null);
      }
    }
  }

  public IndexManager getIndexManager() {
    return _indexManager;
  }

  public BlurShardServer setIndexManager(IndexManager indexManager) {
    this._indexManager = indexManager;
    return this;
  }

  @Override
  public long recordFrequency(ExecutionContext context, String table, String columnFamily, String columnName, String value) throws BlurException, TException {
    checkTable(table);
    long start = context.startTime();
    try {
      checkTableStatus(context, table);
      try {
        return _indexManager.recordFrequency(table, columnFamily, columnName, value);
      } catch (Exception e) {
        LOG.error("Unknown error while trying to get record frequency for [table={0},columnFamily={1},columnName={2},value={3}]", e, table, columnFamily, columnName, value);
        throw new BException(e.getMessage(), e);
      }
    } finally {
      context.recordTime(Metrics.RECORD_FREQUENCY, start, table, columnFamily, columnName, value);
    }
  }

  @Override
  public Schema schema(ExecutionContext context, String table) throws BlurException, TException {
    checkTable(table);
    long start = context.startTime();
    try {
      checkTableStatus(context, table);
      try {
        return _indexManager.schema(table);
      } catch (Exception e) {
        LOG.error("Unknown error while trying to get schema for table [{0}={1}]", e, "table", table);
        throw new BException(e.getMessage(), e);
      }
    } finally {
      context.recordTime(Metrics.SCHEMA, start, table);
    }
  }

  @Override
  public List<String> terms(ExecutionContext context, String table, String columnFamily, String columnName, String startWith, short size) throws BlurException, TException {
    checkTable(table);
    long start = context.startTime();
    try {
      checkTableStatus(context, table);
      try {
        return _indexManager.terms(table, columnFamily, columnName, startWith, size);
      } catch (Exception e) {
        LOG.error("Unknown error while trying to get terms list for [table={0},columnFamily={1},columnName={2},startWith={3},size={4}]", e, table, columnFamily, columnName,
            startWith, size);
        throw new BException(e.getMessage(), e);
      }
    } finally {
      context.recordTime(Metrics.TERMS, start, table, columnFamily, columnName, startWith, size);
    }
  }

  @Override
  public List<String> tableList(ExecutionContext context) throws BlurException, TException {
    try {
      return _clusterStatus.getTableList();
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get a table list.", e);
      throw new BException("Unknown error while trying to get a table list.", e);
    }
  }

  @Override
  public TableDescriptor describe(ExecutionContext context, String table) throws BlurException, TException {
    long start = context.startTime();
    try {
      return _clusterStatus.getTableDescriptor(table);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to describe table [" + table + "]", e);
      throw new BException(e.getMessage(), e);
    } finally {
      context.recordTime(Metrics.DESCRIBE, start, table);
    }
  }

  public IndexServer getIndexServer() {
    return _indexServer;
  }

  public void setIndexServer(IndexServer indexServer) {
    this._indexServer = indexServer;
  }

  @Override
  public List<String> controllerServerList(ExecutionContext context) throws BlurException, TException {
    try {
      return _clusterStatus.getControllerServerList();
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get a controller list.", e);
      throw new BException("Unknown error while trying to get a controller list.", e);
    }
  }

  @Override
  public List<String> shardServerList(ExecutionContext context, String cluster) throws BlurException, TException {
    try {
      return _clusterStatus.getShardServerList(cluster);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get a shard server list.", e);
      throw new BException("Unknown error while trying to get a shard server list.", e);
    }
  }

  @Override
  public void mutate(ExecutionContext context, RowMutation mutation) throws BlurException, TException {
    checkTable(mutation.table);
    long start = context.startTime();
    try {
      MutationHelper.validateMutation(mutation);
      checkTableStatus(context, mutation.table);
      try {
        _indexManager.mutate(mutation);
      } catch (Exception e) {
        LOG.error("Unknown error during processing of [mutation={0}]", e, mutation);
        throw new BException(e.getMessage(), e);
      }
    } finally {
      context.recordTime(Metrics.MUTATE, start, mutation);
    }
  }

  @Override
  public void mutateBatch(ExecutionContext context, List<RowMutation> mutations) throws BlurException, TException {
    for (RowMutation mutation : mutations) {
      MutationHelper.validateMutation(mutation);
    }
    for (RowMutation mutation : mutations) {
      mutate(context, mutation);
    }
  }

  @Override
  public List<String> shardClusterList(ExecutionContext context) throws BlurException, TException {
    try {
      return _clusterStatus.getClusterList();
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get a cluster list.", e);
      throw new BException("Unknown error while trying to get a cluster list.", e);
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

  public void setThreadWatcher(ThreadWatcher threadWatcher) {
    _threadWatcher = threadWatcher;
  }
}
