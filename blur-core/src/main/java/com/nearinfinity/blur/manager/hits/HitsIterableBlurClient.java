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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.BlurSearch;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Client;

public class HitsIterableBlurClient implements HitsIterable {
    
    private static final Log LOG = LogFactory.getLog(HitsIterableBlurClient.class);

    private Map<String, Long> shardInfo = new TreeMap<String, Long>();
    private Client client;
    private String table;
    private String query;
    private boolean superQueryOn;
    private ScoreType type;
    private String postSuperFilter;
    private String preSuperFilter;
    private long minimumNumberOfHits;
    private long maxQueryTime;
    private Hits hits;
    private int fetchCount = 100;
    private int batch = 0;
    private long totalHits;
    private long skipTo;
    private long uuid;

    public HitsIterableBlurClient(BlurSearch.Client client, String table, SearchQuery searchQuery) {
        this.client = client;
        this.table = table;
        this.query = searchQuery.queryStr;
        this.superQueryOn = searchQuery.superQueryOn;
        this.type = searchQuery.type;
        this.postSuperFilter = searchQuery.postSuperFilter;
        this.preSuperFilter = searchQuery.preSuperFilter;
        this.minimumNumberOfHits = searchQuery.minimumNumberOfHits;
        this.maxQueryTime = searchQuery.maxQueryTime;
        this.uuid = searchQuery.uuid;
        performSearch();
    }

    private void performSearch() {
        try {
            long cursor = fetchCount * batch;
            
            SearchQuery searchQuery = new SearchQuery(query, superQueryOn, type, 
                    postSuperFilter, preSuperFilter, cursor, fetchCount, minimumNumberOfHits, 
                    maxQueryTime, uuid, null, false, null);
            
            hits = client.search(table, searchQuery);
            totalHits = hits.totalHits;
            shardInfo.putAll(hits.shardInfo);
            batch++;
        } catch (Exception e) {
            LOG.error("Error during for [{0}]",e,query);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Long> getShardInfo() {
        return shardInfo;
    }

    @Override
    public long getTotalHits() {
        return totalHits;
    }

    @Override
    public void skipTo(long skipTo) {
        this.skipTo = skipTo;
    }

    @Override
    public Iterator<Hit> iterator() {
        SearchIterator iterator = new SearchIterator();
        long start = 0;
        while (iterator.hasNext() && start < skipTo) {
            iterator.next();
            start++;
        }
        return iterator;
    }
    
    public class SearchIterator implements Iterator<Hit> {
        
        private int position = 0;
        private int relposition = 0;

        @Override
        public boolean hasNext() {
            if (position < minimumNumberOfHits && position < totalHits) {
                return true;
            }
            return false;
        }

        @Override
        public Hit next() {
            if (relposition >= hits.hits.size()) {
                performSearch();
                relposition = 0;
            }
            position++;
            return hits.hits.get(relposition++);
        }

        @Override
        public void remove() {
            
        }
    }
}
