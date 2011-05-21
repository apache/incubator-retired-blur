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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLongArray;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.Facet;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.Blur.Client;

public class BlurResultIterableClient implements BlurResultIterable {
    
    private static final Log LOG = LogFactory.getLog(BlurResultIterableClient.class);

    private Map<String, Long> _shardInfo = new TreeMap<String, Long>();
    private Client _client;
    private String _table;
    private String _query;
    private boolean _superQueryOn;
    private ScoreType _type;
    private String _postSuperFilter;
    private String _preSuperFilter;
    private long _minimumNumberOfResults;
    private long _maxQueryTime;
    private BlurResults _results;
    private int _fetchCount = 100;
    private int _batch = 0;
    private long _totalResults;
    private long _skipTo;
    private long _uuid;
    private AtomicLongArray _facetCounts;
    private boolean _alreadyProcessed;
    private String _userId;
    private List<Facet> _facets;

    public BlurResultIterableClient(Blur.Client client, String table, BlurQuery query, AtomicLongArray facetCounts) {
        _client = client;
        _table = table;
        _query = query.queryStr;
        _superQueryOn = query.superQueryOn;
        _type = query.type;
        _postSuperFilter = query.postSuperFilter;
        _preSuperFilter = query.preSuperFilter;
        _minimumNumberOfResults = query.minimumNumberOfResults;
        _maxQueryTime = query.maxQueryTime;
        _uuid = query.uuid;
        _facetCounts = facetCounts;
        _facets = query.facets;
        _userId = query.userId;
        performSearch();
    }

    private void performSearch() {
        try {
            long cursor = _fetchCount * _batch;
            BlurQuery blurQuery = new BlurQuery(_query, _superQueryOn, _type, 
                    _postSuperFilter, _preSuperFilter, cursor, _fetchCount, _minimumNumberOfResults, 
                    _maxQueryTime, _uuid, _userId, false, _facets, null);
            
            _results = _client.query(_table, blurQuery);
            addFacets();
            _totalResults = _results.totalResults;
            _shardInfo.putAll(_results.shardInfo);
            _batch++;
        } catch (Exception e) {
            LOG.error("Error during for [{0}]",e,_query);
            throw new RuntimeException(e);
        }
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
    public Iterator<BlurResult> iterator() {
        SearchIterator iterator = new SearchIterator();
        long start = 0;
        while (iterator.hasNext() && start < _skipTo) {
            iterator.next();
            start++;
        }
        return iterator;
    }
    
    public class SearchIterator implements Iterator<BlurResult> {
        
        private int position = 0;
        private int relposition = 0;

        @Override
        public boolean hasNext() {
            if (position < _minimumNumberOfResults && position < _totalResults) {
                return true;
            }
            return false;
        }

        @Override
        public BlurResult next() {
            if (relposition >= _results.results.size()) {
                performSearch();
                relposition = 0;
            }
            position++;
            return _results.results.get(relposition++);
        }

        @Override
        public void remove() {
            
        }
    }
}
