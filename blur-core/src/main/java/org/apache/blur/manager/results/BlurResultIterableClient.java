package org.apache.blur.manager.results;

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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.BlurClientManager;
import org.apache.blur.thrift.Connection;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.utils.BlurIterator;

public class BlurResultIterableClient implements BlurResultIterable {

  private static final Log LOG = LogFactory.getLog(BlurResultIterableClient.class);

  private final Map<String, Long> _shardInfo = new TreeMap<String, Long>();
  private final Client _client;
  private final String _table;
  private final BlurQuery _originalQuery;
  private final Connection _connection;
  private final int _remoteFetchCount;
  private final AtomicLongArray _facetCounts;

  private BlurResults _results;
  private int _batch = 0;
  private long _totalResults;
  private long _skipTo;
  private boolean _alreadyProcessed;

  public BlurResultIterableClient(Connection connection, Blur.Client client, String table, BlurQuery query,
      AtomicLongArray facetCounts, int remoteFetchCount) throws BlurException {
    _connection = connection;
    _client = client;
    _table = table;
    _facetCounts = facetCounts;
    _originalQuery = query;
    _remoteFetchCount = remoteFetchCount;
    performSearch();
  }

  public Client getClient() {
    return _client;
  }

  private void performSearch() throws BlurException {
    try {
      long cursor = _remoteFetchCount * _batch;
      BlurQuery blurQuery = new BlurQuery(_originalQuery.query, _originalQuery.facets, null,
          _originalQuery.useCacheIfPresent, cursor, _remoteFetchCount, _originalQuery.minimumNumberOfResults,
          _originalQuery.maxQueryTime, _originalQuery.uuid, _originalQuery.userContext, _originalQuery.cacheResult,
          _originalQuery.startTime, _originalQuery.getSortFields(), _originalQuery.getRowId());
      _results = makeLazy(_client.query(_table, blurQuery));
      addFacets();
      _totalResults = _results.totalResults;
      _shardInfo.putAll(_results.shardInfo);
      _batch++;
    } catch (BlurException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Error during for [{0}]", e, _originalQuery);
      throw new RuntimeException(e);
    }
  }

  private BlurResults makeLazy(BlurResults results) {
    List<BlurResult> list = results.results;
    for (int i = 0; i < list.size(); i++) {
      BlurResult blurResult = list.get(i);
      if (blurResult != null) {
        list.set(i, new LazyBlurResult(blurResult, _client));
      }
    }
    return results;
  }

  private void addFacets() {
    if (!_alreadyProcessed) {
      List<Long> counts = _results.facetCounts;
      if (counts != null) {
        int size = counts.size();
        for (int i = 0; i < size; i++) {
          _facetCounts.addAndGet(i, counts.get(i));
        }
      }
      _alreadyProcessed = true;
    }
  }

  @Override
  public Map<String, Long> getShardInfo() {
    return _shardInfo;
  }

  @Override
  public long getTotalResults() {
    return _totalResults;
  }

  @Override
  public void skipTo(long skipTo) {
    this._skipTo = skipTo;
  }

  @Override
  public BlurIterator<BlurResult, BlurException> iterator() throws BlurException {
    SearchIterator iterator = new SearchIterator();
    long start = 0;
    while (iterator.hasNext() && start < _skipTo) {
      iterator.next();
      start++;
    }
    return iterator;
  }

  public class SearchIterator implements BlurIterator<BlurResult, BlurException> {

    private int position = 0;
    private int relposition = 0;

    @Override
    public boolean hasNext() {
      if (position < _originalQuery.minimumNumberOfResults && position < _totalResults) {
        return true;
      }
      return false;
    }

    @Override
    public BlurResult next() throws BlurException {
      if (relposition >= _results.results.size()) {
        performSearch();
        relposition = 0;
      }
      position++;
      return _results.results.get(relposition++);
    }
  }

  @Override
  public void close() throws IOException {
    BlurClientManager.returnClient(_connection, _client);
  }
}
