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

package com.nearinfinity.blur.manager.hits;

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
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.utils.Converter;
import com.nearinfinity.blur.utils.IteratorConverter;

public class HitsIterableSearcher implements HitsIterable {
    
    private Map<String, Long> shardInfo = new TreeMap<String, Long>();
    private String shard;
    private long skipTo;
    private int fetchCount = 1000;

    private IteratorConverter<ScoreDoc, Hit> iterator;
    private Query query;
    private IndexSearcher searcher;
    private TotalHitsRef totalHitsRef = new TotalHitsRef();
    private ProgressRef progressRef = new ProgressRef();

    public HitsIterableSearcher(Query query, String table, String shard, IndexSearcher searcher) throws IOException {
        this.query = query;
        this.shard = shard;
        this.searcher = searcher;
        performSearch();
    }

    private void performSearch() throws IOException {
        IterablePaging iterablePaging = new IterablePaging(searcher, query, fetchCount, totalHitsRef, progressRef);
        iterator = new IteratorConverter<ScoreDoc,Hit>(iterablePaging.iterator(), new Converter<ScoreDoc,Hit>() {
            @Override
            public Hit convert(ScoreDoc scoreDoc) throws Exception {
                return new Hit(resolveId(scoreDoc.doc), null, null, scoreDoc.score, "UNKNOWN");
            }
        });
        shardInfo.put(shard, (long)totalHitsRef.totalHits());
    }

    @Override
    public Map<String, Long> getShardInfo() {
        return shardInfo;
    }

    @Override
    public long getTotalHits() {
        return totalHitsRef.totalHits();
    }

    @Override
    public void skipTo(long skipTo) {
        this.skipTo = skipTo;
    }

    @Override
    public Iterator<Hit> iterator() {
        long start = 0;
        while (iterator.hasNext() && start < skipTo) {
            iterator.next();
            start++;
        }
        return iterator;
    }
    
    private String resolveId(int docId) {
        return shard + "/" + docId;
    }  
}
