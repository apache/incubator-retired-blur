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

package com.nearinfinity.blur.manager.results;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import com.nearinfinity.blur.lucene.search.IterablePaging;
import com.nearinfinity.blur.lucene.search.IterablePaging.ProgressRef;
import com.nearinfinity.blur.lucene.search.IterablePaging.TotalHitsRef;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.utils.Converter;
import com.nearinfinity.blur.utils.IteratorConverter;

public class BlurResultIterableSearcher implements BlurResultIterable {
    
    private Map<String, Long> _shardInfo = new TreeMap<String, Long>();
    private String _shard;
    private long _skipTo;
    private String _table;
    private int _fetchCount = 1000;

    private IteratorConverter<ScoreDoc, BlurResult> _iterator;
    private Selector _selector;
    private Query _query;
    private IndexSearcher _searcher;
    private TotalHitsRef _totalHitsRef = new TotalHitsRef();
    private ProgressRef _progressRef = new ProgressRef();

    public BlurResultIterableSearcher(Query query, String table, String shard, IndexSearcher searcher, Selector selector) throws IOException {
        _table = table;
        _query = query;
        _shard = shard;
        _searcher = searcher;
        _selector = selector;
        performSearch();
    }

    private void performSearch() throws IOException {
        IterablePaging iterablePaging = new IterablePaging(_searcher, _query, _fetchCount, _totalHitsRef, _progressRef);
        _iterator = new IteratorConverter<ScoreDoc,BlurResult>(iterablePaging.iterator(), new Converter<ScoreDoc,BlurResult>() {
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
        IndexManager.fetchRow(_searcher.getIndexReader(), _table, _selector, fetchResult);
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
}
