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
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.Blur.Client;

public class BlurResultIterableClient implements BlurResultIterable {
    
    private static final Log LOG = LogFactory.getLog(BlurResultIterableClient.class);

    private Map<String, Long> shardInfo = new TreeMap<String, Long>();
    private Client client;
    private String table;
    private String query;
    private boolean superQueryOn;
    private ScoreType type;
    private String postSuperFilter;
    private String preSuperFilter;
    private long minimumNumberOfResults;
    private long maxQueryTime;
    private BlurResults hits;
    private int fetchCount = 100;
    private int batch = 0;
    private long totalHits;
    private long skipTo;
    private long uuid;
    private AtomicLongArray facetCounts;

    private boolean alreadyProcessed;

    public BlurResultIterableClient(Blur.Client client, String table, BlurQuery searchQuery, AtomicLongArray facetCounts) {
        this.client = client;
        this.table = table;
        this.query = searchQuery.queryStr;
        this.superQueryOn = searchQuery.superQueryOn;
        this.type = searchQuery.type;
        this.postSuperFilter = searchQuery.postSuperFilter;
        this.preSuperFilter = searchQuery.preSuperFilter;
        this.minimumNumberOfResults = searchQuery.minimumNumberOfResults;
        this.maxQueryTime = searchQuery.maxQueryTime;
        this.uuid = searchQuery.uuid;
        this.facetCounts = facetCounts;
        performSearch();
    }

    private void performSearch() {
        try {
            long cursor = fetchCount * batch;
            
            BlurQuery searchQuery = new BlurQuery(query, superQueryOn, type, 
                    postSuperFilter, preSuperFilter, cursor, fetchCount, minimumNumberOfResults, 
                    maxQueryTime, uuid, null, false, null, null);
            
            hits = client.query(table, searchQuery);
            addFacets();
            totalHits = hits.totalResults;
            shardInfo.putAll(hits.shardInfo);
            batch++;
        } catch (Exception e) {
            LOG.error("Error during for [{0}]",e,query);
            throw new RuntimeException(e);
        }
    }

    private void addFacets() {
        if (!alreadyProcessed) {
            List<Long> counts = hits.facetCounts;
            if (counts != null) {
                int size = counts.size();
                for (int i = 0; i < size; i++) {
                    facetCounts.addAndGet(i, counts.get(i));
                }
            }
            alreadyProcessed = true;
        }
    }

    @Override
    public Map<String, Long> getShardInfo() {
        return shardInfo;
    }

    @Override
    public long getTotalResults() {
        return totalHits;
    }

    @Override
    public void skipTo(long skipTo) {
        this.skipTo = skipTo;
    }

    @Override
    public Iterator<BlurResult> iterator() {
        SearchIterator iterator = new SearchIterator();
        long start = 0;
        while (iterator.hasNext() && start < skipTo) {
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
            if (position < minimumNumberOfResults && position < totalHits) {
                return true;
            }
            return false;
        }

        @Override
        public BlurResult next() {
            if (relposition >= hits.results.size()) {
                performSearch();
                relposition = 0;
            }
            position++;
            return hits.results.get(relposition++);
        }

        @Override
        public void remove() {
            
        }
    }
}
