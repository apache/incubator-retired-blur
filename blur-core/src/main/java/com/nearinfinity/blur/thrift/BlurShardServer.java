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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.lucene.index.IndexReader;
import org.apache.thrift.TException;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.IndexServer.TABLE_STATUS;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurUtil;

public class BlurShardServer implements Iface {

	private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
	private IndexManager indexManager;
	private IndexServer indexServer;
    private boolean closed;
	
    @Override
	public Hits search(String table, SearchQuery searchQuery) throws BlurException, TException {
        checkTableStatus(table);
        try {
            AtomicLongArray facetCounts = BlurUtil.getAtomicLongArraySameLengthAsList(searchQuery.facets);
            HitsIterable hitsIterable = indexManager.search(table, searchQuery, facetCounts);
            return BlurBaseServer.convertToHits(hitsIterable,searchQuery.start,searchQuery.fetch,searchQuery.minimumNumberOfHits, facetCounts);
        } catch (BlurException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unknown error during search of [table={0},searchQuery={1}]", e, table, searchQuery);
            throw new BlurException(e.getMessage());
        }
	}
	
    @Override
	public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
	    checkTableStatus(table);
        try {
            FetchResult fetchResult = new FetchResult();
            indexManager.fetchRow(table,selector, fetchResult);
            return fetchResult;
        } catch (BlurException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get fetch row [table={0},selector={1}]",e,table,selector);
            throw new BlurException(e.getMessage());
        }
	}

    @Override
    public void cancelSearch(String table, long uuid) throws BlurException, TException {
        try {
            indexManager.cancelSearch(table, uuid);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to cancel search [uuid={0}]",e,uuid);
            throw new BlurException(e.getMessage());
        }
    }

    @Override
    public List<SearchQueryStatus> currentSearches(String table) throws BlurException, TException {
        checkTableStatus(table);
        try {
            return indexManager.currentSearches(table);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get current search status [table={0}]",e,table);
            throw new BlurException(e.getMessage());
        }
    }
    
    public synchronized void close() {
        if (!closed) {
            closed = true;
            indexManager.close();
        }
    }
    
    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        checkTableStatus(table);
        try {
            Map<String, IndexReader> indexReaders = indexServer.getIndexReaders(table);
            Map<String, String> result = new TreeMap<String, String>();
            String nodeName = indexServer.getNodeName();
            for (String shard : indexReaders.keySet()) {
                result.put(shard, nodeName);
            }
            return result;
        } catch (Exception e) {
            if (e instanceof BlurException) {
                throw (BlurException) e;
            }
            LOG.error("Unknown error while trying to getting shardServerLayout for table [" + table + "]",e);
            throw new BlurException(e.getMessage());
        }
    }
    
    private void checkTableStatus(String table) throws BlurException {
        if (!isTableEnabled(table)) {
            throw new BlurException("Table [" + table + "] is disabled.");
        }
    }

    public IndexManager getIndexManager() {
        return indexManager;
    }

    public BlurShardServer setIndexManager(IndexManager indexManager) {
        this.indexManager = indexManager;
        return this;
    }

    @Override
    public long recordFrequency(String table, String columnFamily, String columnName, String value) throws BlurException, TException {
        checkTableStatus(table);
        try {
            return indexManager.recordFrequency(table,columnFamily,columnName,value);
        } catch (BlurException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get record frequency for [table={0},columnFamily={1},columnName={2},value={3}]",e,table,columnFamily,columnName,value);
            throw new BlurException(e.getMessage());
        }
    }

    @Override
    public Schema schema(String table) throws BlurException, TException {
        checkTableStatus(table);
        try {
            return indexManager.schema(table);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get schema for table [{0}={1}]",e,"table",table);
            throw new BlurException(e.getMessage());
        }
    }

    @Override
    public List<String> terms(String table, String columnFamily, String columnName, String startWith, short size) throws BlurException, TException {
        checkTableStatus(table);
        try {
            return indexManager.terms(table,columnFamily,columnName,startWith,size);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get terms list for [table={0},columnFamily={1},columnName={2},startWith={3},size={4}]",e,table,columnFamily,columnName,startWith,size);
            throw new BlurException(e.getMessage());
        }
    }
    
    @Override
    public List<String> tableList() throws BlurException, TException {
        return indexServer.getTableList();
    }
    
    @Override
    public TableDescriptor describe(String table) throws BlurException, TException {
        try {
            TableDescriptor descriptor = new TableDescriptor();
            descriptor.analyzerDef = indexServer.getAnalyzer(table).toString();
            descriptor.shardNames = new ArrayList<String>(indexServer.getShardServerList());
            descriptor.isEnabled = isTableEnabled(table);
            return descriptor;
        } catch (Exception e) {
            LOG.error("Unknown error while trying to describe table [" + table + "]", e);
            throw new BlurException(e.getMessage());
        }
    }

    public boolean isTableEnabled(String table) {
        TABLE_STATUS tableStatus = indexServer.getTableStatus(table);
        if (tableStatus == TABLE_STATUS.ENABLED) {
            return true;
        } else {
            return false;
        }
    }
    
    public IndexServer getIndexServer() {
        return indexServer;
    }

    public void setIndexServer(IndexServer indexServer) {
        this.indexServer = indexServer;
    }
    
    @Override
    public List<String> controllerServerList() throws BlurException, TException {
        return indexServer.getControllerServerList();
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        return indexServer.getOnlineShardServers();
    }

    @Override
    public void mutate(List<RowMutation> mutations) throws BlurException, TException {
        throw new RuntimeException("not impl");
    }
}
