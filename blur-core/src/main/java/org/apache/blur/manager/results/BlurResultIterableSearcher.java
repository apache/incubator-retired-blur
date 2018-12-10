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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.lucene.search.DeepPagingCache;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.lucene.search.IterablePaging;
import org.apache.blur.lucene.search.IterablePaging.ProgressRef;
import org.apache.blur.lucene.search.IterablePaging.TotalHitsRef;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.utils.BlurIterator;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.Converter;
import org.apache.blur.utils.IteratorConverter;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;

public class BlurResultIterableSearcher implements BlurResultIterable {

  private final Map<String, Long> _shardInfo = new TreeMap<String, Long>();
  private final String _shard;
  private final int _fetchCount;
  private final IteratorConverter<ScoreDoc, BlurResult, BlurException> _iteratorConverter;
  private final Query _query;
  private final TotalHitsRef _totalHitsRef = new TotalHitsRef();
  private final ProgressRef _progressRef = new ProgressRef();
  private final AtomicBoolean _running;
  private final boolean _closeSearcher;
  private final boolean _runSlow;
  private final IterablePaging _iterablePaging;
  private final Sort _sort;

  private IndexSearcherCloseable _searcher;
  private long _skipTo;

  public BlurResultIterableSearcher(AtomicBoolean running, Query query, String table, String shard,
      IndexSearcherCloseable searcher, Selector selector, boolean closeSearcher, boolean runSlow, int fetchCount,
      int maxHeapPerRowFetch, TableContext context, Sort sort, DeepPagingCache deepPagingCache) throws BlurException {
    _sort = sort;
    _running = running;
    _query = query;
    _shard = shard;
    _searcher = searcher;
    _closeSearcher = closeSearcher;
    _runSlow = runSlow;
    _fetchCount = fetchCount;
    _iterablePaging = new IterablePaging(_running, _searcher, _query, _fetchCount, _totalHitsRef, _progressRef,
        _runSlow, _sort, deepPagingCache);
    _iteratorConverter = new IteratorConverter<ScoreDoc, BlurResult, BlurException>(_iterablePaging.iterator(),
        new Converter<ScoreDoc, BlurResult, BlurException>() {
          @Override
          public BlurResult convert(ScoreDoc scoreDoc) throws BlurException {
            String resolveId = resolveId(scoreDoc);
            if (_sort == null) {
              return new BlurResult(resolveId, scoreDoc.score, null, null);
            } else {
              FieldDoc fieldDoc = (FieldDoc) scoreDoc;
              return new BlurResult(resolveId, scoreDoc.score, null, BlurUtil.convertToSortFields(fieldDoc.fields));
            }
          }
        });
    _shardInfo.put(_shard, (long) _totalHitsRef.totalHits());
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
  public BlurIterator<BlurResult, BlurException> iterator() throws BlurException {
    _iterablePaging.skipTo((int) _skipTo);
    return _iteratorConverter;
  }

  private String resolveId(ScoreDoc scoreDoc) {
    return _shard + "/" + scoreDoc.doc;
  }

  @Override
  public void close() throws IOException {
    if (_searcher != null && _closeSearcher) {
      _searcher.close();
      _searcher = null;
    }
  }
}
