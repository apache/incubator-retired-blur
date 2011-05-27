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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.concurrent.ExecutorsDynamicConfig;
import com.nearinfinity.blur.concurrent.SimpleExecutorsDynamicConfig;
import com.nearinfinity.blur.manager.indexserver.ClusterStatus;
import com.nearinfinity.blur.thrift.client.BlurClient;
import com.nearinfinity.blur.thrift.client.BlurClientEmbedded;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.BlurQuerySuggestions;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.Facet;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.FetchRowResult;
import com.nearinfinity.blur.thrift.generated.RecordMutation;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class BlurControllerServerTest {
    
    private static final String TABLE = "test";
    private Map<String,Iface> shardServers = new HashMap<String, Iface>();
    private BlurControllerServer server;
    private ExecutorsDynamicConfig dynamicConfig;
    
    @Before
    public void setup() {
        dynamicConfig = new SimpleExecutorsDynamicConfig(10);
        addShardServer("shard-00000000");
        addShardServer("shard-00000001");
        addShardServer("shard-00000002");
        server = new BlurControllerServer();
        server.setClient(getClient());
        server.setClusterStatus(getClusterStatus());
        server.setDynamicConfig(dynamicConfig);
        server.open();
    }
    
    private ClusterStatus getClusterStatus() {
        return new ClusterStatus() {

            @Override
            public List<String> controllerServerList() {
                throw new RuntimeException("no impl");
            }

            @Override
            public List<String> getOnlineShardServers() {
                return shardServerList();
            }

            @Override
            public List<String> shardServerList() {
                List<String> nodes = new ArrayList<String>(shardServers.keySet());
                Collections.sort(nodes);
                return nodes;
            }
            
        };
    }

    @After
    public void tearDown() {
        server.close();
    }

    @Test
    public void testQuery() throws BlurException, TException {
        BlurQuery blurQuery = new BlurQuery();
        blurQuery.maxQueryTime = TimeUnit.SECONDS.toMillis(5);
        blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
        BlurResults results = server.query(TABLE, blurQuery);
        assertNotNull(results);
    }
    
    @Test
    public void testQueryWithFacets() throws BlurException, TException {
        BlurQuery blurQuery = new BlurQuery();
        blurQuery.maxQueryTime = TimeUnit.SECONDS.toMillis(5);
        blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
        blurQuery.facets = new ArrayList<Facet>();
        blurQuery.facets.add(new Facet());
        blurQuery.facets.add(new Facet());
        BlurResults results = server.query(TABLE, blurQuery);
        assertNotNull(results);
        assertNotNull(results.facetCounts);
        for (int i = 0; i < results.facetCounts.size(); i++) {
            long count = results.facetCounts.get(i);
            assertEquals(shardServers.size() * (i+1), count);
        }
    }
    
    @Test
    public void testRecordFrequency() throws BlurException, TException {
        long recordFrequency = server.recordFrequency(TABLE, "cf", "cn", "value");
        assertEquals(shardServers.size(),recordFrequency);
    }
    
    @Test
    public void testMutate() throws BlurException, TException {
        RowMutation mutation = new RowMutation();
        mutation.setRowId("1234");
        RecordMutation recMut = new RecordMutation();
        recMut.setFamily("test");
        recMut.setRecordId("5678");
        recMut.addToRecord(new Column("name",Arrays.asList("value")));
        mutation.addToRecordMutations(recMut);
        server.mutate(TABLE, Arrays.asList(mutation));
        
        Selector selector = new Selector();
        selector.rowId = "1234";
        
        FetchResult fetchRow = server.fetchRow(TABLE, selector);
        assertNotNull(fetchRow.rowResult);
    }

    private BlurClient getClient() {
        BlurClientEmbedded blurClientEmbedded = new BlurClientEmbedded();
        for (String node : shardServers.keySet()) {
            blurClientEmbedded.putNode(node, shardServers.get(node));
        }
        return blurClientEmbedded;
    }

    
    private Iface getShardServer(final String node) {
        return new Iface() {
            
            private Map<String,Map<String,Row>> rows = new HashMap<String, Map<String,Row>>();
            
            @Override
            public List<String> terms(String arg0, String arg1, String arg2, String arg3, short arg4) throws BlurException,
                    TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public List<String> tableList() throws BlurException, TException {
                List<String> table = new ArrayList<String>();
                table.add(TABLE);
                return table;
            }
            
            @Override
            public List<String> shardServerList() throws BlurException, TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
                Map<String,String> layout = new HashMap<String, String>();
                layout.put(node, node);
                return layout;
            }
            
            @Override
            public BlurResults query(String table, BlurQuery query) throws BlurException, TException {
                BlurResults results = new BlurResults();
                results.putToShardInfo(node, 0);
                results.setFacetCounts(getFacetCounts(query));
                return results;
            }
            
            private List<Long> getFacetCounts(BlurQuery query) {
                if (query.facets != null) {
                    int size = query.facets.size();
                    List<Long> results = new ArrayList<Long>();
                    for (int i = 0; i < size; i++) {
                        results.add(i + 1L);
                    }
                    return results;
                }
                return null;
            }

            @Override
            public Schema schema(String arg0) throws BlurException, TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public long recordFrequency(String arg0, String arg1, String arg2, String arg3) throws BlurException, TException {
                return 1l;
            }
            
            @Override
            public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
                Map<String, Row> map = rows.get(table);
                Row row = map.get(selector.rowId);
                FetchResult fetchResult = new FetchResult();
                fetchResult.setRowResult(new FetchRowResult(row));
                return fetchResult;
            }
            
            @Override
            public TableDescriptor describe(String arg0) throws BlurException, TException {
                TableDescriptor descriptor = new TableDescriptor();
                descriptor.isEnabled = true;
                descriptor.shardCount = 3;
                return descriptor;
            }
            
            @Override
            public List<BlurQueryStatus> currentQueries(String arg0) throws BlurException, TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public List<String> controllerServerList() throws BlurException, TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public void cancelQuery(String table, long arg0) throws BlurException, TException {
                throw new RuntimeException("no impl");                
            }

            @Override
            public void mutate(String table, List<RowMutation> mutations) throws BlurException, TException {
                Map<String, Row> map = rows.get(table);
                if (map == null) {
                    map = new HashMap<String, Row>();
                    rows.put(table, map);
                }
                for (RowMutation mutation : mutations) {
                    Row row = toRow(mutation);
                    map.put(row.id, row);
                }
            }

            private Row toRow(RowMutation mutation) {
                Row row = new Row();
                row.id = mutation.rowId;
                row.columnFamilies = toColumnFamilies(mutation.recordMutations);
                return row;
            }

            private Set<ColumnFamily> toColumnFamilies(List<RecordMutation> recordMutations) {
                Map<String,ColumnFamily> cfs = new HashMap<String, ColumnFamily>();
                for (RecordMutation mutation : recordMutations) {
                    String family = mutation.family;
                    ColumnFamily columnFamily = cfs.get(family);
                    if (columnFamily == null) {
                        columnFamily = new ColumnFamily();
                        cfs.put(family, columnFamily);
                    }
                    columnFamily.putToRecords(mutation.recordId, mutation.record);
                }
                return new HashSet<ColumnFamily>(cfs.values());
            }

            @Override
            public BlurQuerySuggestions querySuggestions(String table, BlurQuery blurQuery) throws BlurException,
                    TException {
                throw new RuntimeException("not impl");
            }

            @Override
            public void createTable(String table, boolean enabled, String analyzerDef, int shardCount, String tableUri)
                    throws BlurException, TException {
                throw new RuntimeException("not impl");
            }

            @Override
            public void disableTable(String table) throws BlurException, TException {
                throw new RuntimeException("not impl");
            }

            @Override
            public void enableTable(String table) throws BlurException, TException {
                throw new RuntimeException("not impl");
            }

            @Override
            public void removeTable(String table, boolean deleteIndexFiles) throws BlurException, TException {
                throw new RuntimeException("not impl");
            }
        };
    }
    
    private void addShardServer(String node) {
        shardServers.put(node, getShardServer(node));
    }
}
