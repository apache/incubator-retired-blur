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
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.lucene.search.IterablePaging;
import org.apache.blur.lucene.search.IterablePaging.ProgressRef;
import org.apache.blur.lucene.search.IterablePaging.TotalHitsRef;
import org.apache.blur.manager.IndexManager;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.utils.Converter;
import org.apache.blur.utils.IteratorConverter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;


public class BlurResultIterableSearcher implements BlurResultIterable {

  private Map<String, Long> _shardInfo = new TreeMap<String, Long>();
  private String _shard;
  private long _skipTo;
  private String _table;
  private int _fetchCount = 1000;

  private IteratorConverter<ScoreDoc, BlurResult> _iterator;
  private final Selector _selector;
  private final Query _query;
  private IndexSearcherClosable _searcher;
  private final TotalHitsRef _totalHitsRef = new TotalHitsRef();
  private final ProgressRef _progressRef = new ProgressRef();
  private final AtomicBoolean _running;
  private final boolean _closeSearcher;

  public BlurResultIterableSearcher(AtomicBoolean running, Query query, String table, String shard, IndexSearcherClosable searcher, Selector selector, boolean closeSearcher)
      throws IOException {
    _running = running;
    _table = table;
    _query = query;
    _shard = shard;
    _searcher = searcher;
    _selector = selector;
    _closeSearcher = closeSearcher;
    performSearch();
  }

  private void performSearch() throws IOException {
    IterablePaging iterablePaging = new IterablePaging(_running, _searcher, _query, _fetchCount, _totalHitsRef, _progressRef);
    _iterator = new IteratorConverter<ScoreDoc, BlurResult>(iterablePaging.iterator(), new Converter<ScoreDoc, BlurResult>() {
      @Override
      public BlurResult convert(ScoreDoc scoreDoc) throws Exception {
        String resolveId = resolveId(scoreDoc.doc);
        return new BlurResult(resolveId, scoreDoc.score, getFetchResult(resolveId));
      }
    });
    _shardInfo.put(_shard, (long) _totalHitsRef.totalHits());
  }

  private FetchResult getFetchResult(String resolveId) throws IOException, BlurException {
    if (_selector == null) {
      return null;
    }
    FetchResult fetchResult = new FetchResult();
    _selector.setLocationId(resolveId);
    IndexManager.validSelector(_selector);
    IndexManager.fetchRow(_searcher.getIndexReader(), _table, _selector, fetchResult, null);
    return fetchResult;
  }

  @Override
  public Map<String, Long> getShardInfo() {
    return _shardInfo;
  }

  @Override
  public long getTotalResults() {
    return _totalHitsRef.totalHits();
  }

  @Override
  public void skipTo(long skipTo) {
    _skipTo = skipTo;
  }

  @Override
  public Iterator<BlurResult> iterator() {
    long start = 0;
    while (_iterator.hasNext() && start < _skipTo) {
      _iterator.next();
      start++;
    }
    return _iterator;
  }

  private String resolveId(int docId) {
    return _shard + "/" + docId;
  }

  @Override
  public void close() throws IOException {
    if (_searcher != null && _closeSearcher) {
      _searcher.close();
      _searcher = null;
    }
  }
}
