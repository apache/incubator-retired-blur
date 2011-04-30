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

import org.apache.thrift.TException;

import com.nearinfinity.blur.concurrent.ExecutionContext;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.IndexServer.TABLE_STATUS;
import com.nearinfinity.blur.manager.results.BlurResultIterable;
import com.nearinfinity.blur.manager.writer.BlurIndex;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.BlurQuerySuggestions;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurUtil;

public class BlurShardServer extends ExecutionContextIface {

    private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
    private IndexManager indexManager;
    private IndexServer indexServer;
    private boolean closed;

    public enum Metrics {
        GET_TABLE_STATUS, QUERY, FETCH_ROW, SHARD_SERVER_LAYOUT, CANCEL_QUERY, CURRENT_QUERIES, RECORD_FREQUENCY, SCHEMA, TERMS, DESCRIBE, MUTATE, QUERY_SUGGESTIONS
    }

    @Override
    public BlurResults query(ExecutionContext context, String table, BlurQuery blurQuery) throws BlurException,
            TException {
        long start = context.startTime();
        try {
            checkTableStatus(context, table);
            try {
                AtomicLongArray facetCounts = BlurUtil.getAtomicLongArraySameLengthAsList(blurQuery.facets);
                BlurResultIterable hitsIterable = indexManager.query(table, blurQuery, facetCounts);
                return BlurBaseServer.convertToHits(hitsIterable, blurQuery.start, blurQuery.fetch,
                        blurQuery.minimumNumberOfResults, facetCounts);
            } catch (Exception e) {
                LOG.error("Unknown error during search of [table={0},searchQuery={1}]", e, table, blurQuery);
                throw new BException(e.getMessage(), e);
            }
        } finally {
            context.recordTime(Metrics.QUERY, start, table, blurQuery);
        }
    }

    @Override
    public FetchResult fetchRow(ExecutionContext context, String table, Selector selector) throws BlurException,
            TException {
        long start = context.startTime();
        try {
            checkTableStatus(context, table);
            try {
                FetchResult fetchResult = new FetchResult();
                indexManager.fetchRow(table, selector, fetchResult);
                return fetchResult;
            } catch (Exception e) {
                LOG.error("Unknown error while trying to get fetch row [table={0},selector={1}]", e, table, selector);
                throw new BException(e.getMessage(), e);
            }
        } finally {
            context.recordTime(Metrics.FETCH_ROW, start, table, selector);
        }
    }

    @Override
    public void cancelQuery(ExecutionContext context, String table, long uuid) throws BlurException, TException {
        long start = context.startTime();
        try {
            indexManager.cancelQuery(table, uuid);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to cancel search [uuid={0}]", e, uuid);
            throw new BException(e.getMessage(), e);
        } finally {
            context.recordTime(Metrics.CANCEL_QUERY, start, table, uuid);
        }
    }

    @Override
    public List<BlurQueryStatus> currentQueries(ExecutionContext context, String table) throws BlurException,
            TException {
        long start = context.startTime();
        try {
            checkTableStatus(context, table);
            try {
                return indexManager.currentQueries(table);
            } catch (Exception e) {
                LOG.error("Unknown error while trying to get current search status [table={0}]", e, table);
                throw new BException(e.getMessage(), e);
            }
        } finally {
            context.recordTime(Metrics.CURRENT_QUERIES, start, table);
        }
    }

    public synchronized void close() {
        if (!closed) {
            closed = true;
            indexManager.close();
        }
    }

    @Override
    public Map<String, String> shardServerLayout(ExecutionContext context, String table) throws BlurException,
            TException {
        long start = context.startTime();
        try {
            checkTableStatus(context, table);
            try {
                Map<String, BlurIndex> blurIndexes = indexServer.getIndexes(table);
                Map<String, String> result = new TreeMap<String, String>();
                String nodeName = indexServer.getNodeName();
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
        } finally {
            context.recordTime(Metrics.SHARD_SERVER_LAYOUT, start, table);
        }
    }

    private void checkTableStatus(ExecutionContext context, String table) throws BlurException {
        if (!isTableEnabled(context, table)) {
            throw new BlurException("Table [" + table + "] is disabled.", null);
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
    public long recordFrequency(ExecutionContext context, String table, String columnFamily, String columnName,
            String value) throws BlurException, TException {
        long start = context.startTime();
        try {
            checkTableStatus(context, table);
            try {
                return indexManager.recordFrequency(table, columnFamily, columnName, value);
            } catch (Exception e) {
                LOG
                        .error(
                                "Unknown error while trying to get record frequency for [table={0},columnFamily={1},columnName={2},value={3}]",
                                e, table, columnFamily, columnName, value);
                throw new BException(e.getMessage(), e);
            }
        } finally {
            context.recordTime(Metrics.RECORD_FREQUENCY, start, table, columnFamily, columnName, value);
        }
    }

    @Override
    public Schema schema(ExecutionContext context, String table) throws BlurException, TException {
        long start = context.startTime();
        try {
            checkTableStatus(context, table);
            try {
                return indexManager.schema(table);
            } catch (Exception e) {
                LOG.error("Unknown error while trying to get schema for table [{0}={1}]", e, "table", table);
                throw new BException(e.getMessage(), e);
            }
        } finally {
            context.recordTime(Metrics.SCHEMA, start, table);
        }
    }

    @Override
    public List<String> terms(ExecutionContext context, String table, String columnFamily, String columnName,
            String startWith, short size) throws BlurException, TException {
        long start = context.startTime();
        try {
            checkTableStatus(context, table);
            try {
                return indexManager.terms(table, columnFamily, columnName, startWith, size);
            } catch (Exception e) {
                LOG
                        .error(
                                "Unknown error while trying to get terms list for [table={0},columnFamily={1},columnName={2},startWith={3},size={4}]",
                                e, table, columnFamily, columnName, startWith, size);
                throw new BException(e.getMessage(), e);
            }
        } finally {
            context.recordTime(Metrics.TERMS, start, table, columnFamily, columnName, startWith, size);
        }
    }

    @Override
    public List<String> tableList(ExecutionContext context) throws BlurException, TException {
        return indexServer.getTableList();
    }

    @Override
    public TableDescriptor describe(ExecutionContext context, String table) throws BlurException, TException {
        long start = context.startTime();
        try {
            TableDescriptor descriptor = new TableDescriptor();
            descriptor.analyzerDef = indexServer.getAnalyzer(table).toString();
            boolean tableEnabled = isTableEnabled(context, table);
            if (tableEnabled) {
                Map<String, BlurIndex> indexes = indexServer.getIndexes(table);
                descriptor.shardNames = new ArrayList<String>(indexes.keySet());
            }
            descriptor.isEnabled = tableEnabled;
            return descriptor;
        } catch (Exception e) {
            LOG.error("Unknown error while trying to describe table [" + table + "]", e);
            throw new BException(e.getMessage(), e);
        } finally {
            context.recordTime(Metrics.DESCRIBE, start, table);
        }
    }

    public boolean isTableEnabled(ExecutionContext context, String table) {
        long start = context.startTime();
        try {
            TABLE_STATUS tableStatus = indexServer.getTableStatus(table);
            if (tableStatus == TABLE_STATUS.ENABLED) {
                return true;
            } else {
                return false;
            }
        } finally {
            context.recordTime(Metrics.GET_TABLE_STATUS, start, table);
        }
    }

    public IndexServer getIndexServer() {
        return indexServer;
    }

    public void setIndexServer(IndexServer indexServer) {
        this.indexServer = indexServer;
    }

    @Override
    public List<String> controllerServerList(ExecutionContext context) throws BlurException, TException {
        return indexServer.getControllerServerList();
    }

    @Override
    public List<String> shardServerList(ExecutionContext context) throws BlurException, TException {
        return indexServer.getOnlineShardServers();
    }

    @Override
    public void mutate(ExecutionContext context, String table, List<RowMutation> mutations) throws BlurException,
            TException {
        long start = context.startTime();
        try {
            checkTableStatus(context, table);
            try {
                indexManager.mutate(table, mutations);
            } catch (Exception e) {
                LOG.error("Unknown error during processing of [table={0},mutations={1}]", e, table, mutations);
                throw new BException(e.getMessage(), e);
            }
        } finally {
            context.recordTime(Metrics.MUTATE, start, table, mutations);
        }
    }

    @Override
    public BlurQuerySuggestions querySuggestions(ExecutionContext context, String table, BlurQuery blurQuery)
            throws BlurException, TException {
        long start = context.startTime();
        try {
            throw new RuntimeException("not impl");
        } finally {
            context.recordTime(Metrics.QUERY_SUGGESTIONS, start, table, blurQuery);
        }
    }
}
