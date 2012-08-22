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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.thrift.TException;

import com.nearinfinity.blur.concurrent.Executors;
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
import com.nearinfinity.blur.thrift.generated.TableStats;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurUtil;
import com.nearinfinity.blur.utils.QueryCache;
import com.nearinfinity.blur.utils.QueryCacheEntry;
import com.nearinfinity.blur.utils.QueryCacheKey;

public class BlurShardServer extends TableAdmin implements Iface {

  private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
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

  public void init() {
    _queryCache = new QueryCache("shard-cache", _maxQueryCacheElements, _maxTimeToLive);
    _dataFetch = Executors.newThreadPool("data-fetch-", _dataFetchThreadCount);
  }

  @Override
  public BlurResults query(String table, BlurQuery blurQuery) throws BlurException, TException {
    checkTable(_cluster, table);
    _queryChecker.checkQuery(blurQuery);
    try {
      BlurQuery original = new BlurQuery(blurQuery);
      if (blurQuery.useCacheIfPresent) {
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
      BlurUtil.setStartTime(original);
      BlurResultIterable hitsIterable = null;
      try {
        AtomicLongArray facetCounts = BlurUtil.getAtomicLongArraySameLengthAsList(blurQuery.facets);
        hitsIterable = _indexManager.query(table, blurQuery, facetCounts);
        return _queryCache.cache(table, original, BlurUtil.convertToHits(hitsIterable, blurQuery, facetCounts, _dataFetch, blurQuery.selector, this, table));
      } catch (Exception e) {
        LOG.error("Unknown error during search of [table={0},searchQuery={1}]", e, table, blurQuery);
        throw new BException(e.getMessage(), e);
      } finally {
        if (hitsIterable != null) {
          hitsIterable.close();
        }
      }
    } catch (IOException e) {
      LOG.error("Unknown error during search of [table={0},searchQuery={1}]", e, table, blurQuery);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
    checkTable(_cluster, table);
    try {
      FetchResult fetchResult = new FetchResult();
      _indexManager.fetchRow(table, selector, fetchResult);
      return fetchResult;
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get fetch row [table={0},selector={1}]", e, table, selector);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void cancelQuery(String table, long uuid) throws BlurException, TException {
    checkTable(_cluster, table);
    try {
      _indexManager.cancelQuery(table, uuid);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to cancel search [uuid={0}]", e, uuid);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public List<BlurQueryStatus> currentQueries(String table) throws BlurException, TException {
    checkTable(_cluster, table);
    try {
      return _indexManager.currentQueries(table);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get current search status [table={0}]", e, table);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public TableStats tableStats(String table) throws BlurException, TException {
    checkTable(_cluster, table);
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
    checkTable(_cluster, table);
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
  }

  @Override
  public long recordFrequency(String table, String columnFamily, String columnName, String value) throws BlurException, TException {
    checkTable(_cluster, table);
    try {
      return _indexManager.recordFrequency(table, columnFamily, columnName, value);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get record frequency for [table={0},columnFamily={1},columnName={2},value={3}]", e, table, columnFamily, columnName, value);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public Schema schema(String table) throws BlurException, TException {
    checkTable(_cluster, table);
    try {
      return _indexManager.schema(table);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get schema for table [{0}={1}]", e, "table", table);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public List<String> terms(String table, String columnFamily, String columnName, String startWith, short size) throws BlurException, TException {
    checkTable(_cluster, table);
    try {
      return _indexManager.terms(table, columnFamily, columnName, startWith, size);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get terms list for [table={0},columnFamily={1},columnName={2},startWith={3},size={4}]", e, table, columnFamily, columnName,
          startWith, size);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void mutate(RowMutation mutation) throws BlurException, TException {
    checkTable(_cluster, mutation.table);
    checkForUpdates(_cluster, mutation.table);
    MutationHelper.validateMutation(mutation);
    try {
      _indexManager.mutate(mutation);
    } catch (Exception e) {
      LOG.error("Unknown error during processing of [mutation={0}]", e, mutation);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void mutateBatch(List<RowMutation> mutations) throws BlurException, TException {
    for (RowMutation mutation : mutations) {
      checkTable(_cluster, mutation.table);
      checkForUpdates(_cluster, mutation.table);
      MutationHelper.validateMutation(mutation);
    }
    try {
      _indexManager.mutate(mutations);
    } catch (Exception e) {
      LOG.error("Unknown error during processing of [mutations={0}]", e, mutations);
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
  public BlurQueryStatus queryStatusById(String table, long uuid) throws BlurException, TException {
    checkTable(_cluster, table);
    try {
      return _indexManager.queryStatus(table, uuid);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get current query status [table={0},uuid={1}]", e, table, uuid);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public List<Long> queryStatusIdList(String table) throws BlurException, TException {
    checkTable(_cluster, table);
    try {
      return _indexManager.queryStatusIdList(table);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get query status id list [table={0}]", e, table);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void optimize(String table, int numberOfSegmentsPerShard) throws BlurException, TException {
    checkTable(_cluster, table);
    try {
      _indexManager.optimize(table, numberOfSegmentsPerShard);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to optimize [table={0},numberOfSegmentsPerShard={1}]", e, table, numberOfSegmentsPerShard);
      throw new BException(e.getMessage(), e);
    }
  }

  public int getDataFetchThreadCount() {
    return _dataFetchThreadCount;
  }

  public void setDataFetchThreadCount(int dataFetchThreadCount) {
    _dataFetchThreadCount = dataFetchThreadCount;
  }
}
