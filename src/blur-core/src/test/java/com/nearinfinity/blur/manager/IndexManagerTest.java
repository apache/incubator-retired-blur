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

package com.nearinfinity.blur.manager;

import static com.nearinfinity.blur.utils.BlurUtil.newColumn;
import static com.nearinfinity.blur.utils.BlurUtil.newColumnFamily;
import static com.nearinfinity.blur.utils.BlurUtil.newRecordMutation;
import static com.nearinfinity.blur.utils.BlurUtil.newRow;
import static com.nearinfinity.blur.utils.BlurUtil.newRowMutation;
import static com.nearinfinity.blur.utils.BlurUtil.newRowMutations;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLongArray;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.BlurShardName;
import com.nearinfinity.blur.concurrent.ExecutorsDynamicConfig;
import com.nearinfinity.blur.concurrent.SimpleExecutorsDynamicConfig;
import com.nearinfinity.blur.manager.indexserver.LocalIndexServer;
import com.nearinfinity.blur.manager.results.BlurResultIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Facet;
import com.nearinfinity.blur.thrift.generated.FetchRecordResult;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.utils.BlurConstants;

public class IndexManagerTest {

    private static final String SHARD_NAME = BlurShardName.getShardName(BlurConstants.SHARD_PREFIX, 0);
    private static final String TABLE = "table";
    private IndexServer server;
    private IndexManager indexManager;
    private ExecutorsDynamicConfig dynamicConfig;

    @Before
    public void setUp() throws BlurException, IOException {
        File file = new File("./tmp/indexer-manager-test");
        rm(file);
        new File(new File(file, TABLE), SHARD_NAME).mkdirs();
        dynamicConfig = new SimpleExecutorsDynamicConfig(10);
        server = new LocalIndexServer(file);
        indexManager = new IndexManager();
        indexManager.setStatusCleanupTimerDelay(1000);
        indexManager.setIndexServer(server);
        indexManager.setDynamicConfig(dynamicConfig);
        indexManager.init();
        setupData();
    }

    private void rm(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
    }

    private void setupData() throws BlurException, IOException {
        List<RowMutation> mutations = newRowMutations(
                newRowMutation("row-1", 
                        newRecordMutation("test-family", "record-1", 
                                newColumn("testcol1", "value1"), 
                                newColumn("testcol2", "value2"), 
                                newColumn("testcol3", "value3"))), 
                newRowMutation("row-2", 
                        newRecordMutation("test-family", "record-2", 
                                newColumn("testcol1", "value4"), 
                                newColumn("testcol2", "value5"), 
                                newColumn("testcol3", "value6"))),
                newRowMutation("row-3", 
                        newRecordMutation("test-family", "record-3", 
                                newColumn("testcol1", "value7"),
                                newColumn("testcol2", "value8"), 
                                newColumn("testcol3", "value9"))),
                newRowMutation("row-4", 
                        newRecordMutation("test-family", "record-4", 
                                newColumn("testcol1", "value1"),
                                newColumn("testcol2", "value5"), 
                                newColumn("testcol3", "value9"))));
        indexManager.mutate(TABLE, mutations);
    }

    @After
    public void tearDown() throws InterruptedException {
        indexManager.close();
    }

    @Test
    public void testFetchRowByLocationId() throws Exception {
        Selector selector = new Selector().setLocationId(SHARD_NAME + "/0");
        FetchResult fetchResult = new FetchResult();
        indexManager.fetchRow(TABLE, selector, fetchResult);
        assertNotNull(fetchResult.rowResult.row);
        Row row = newRow("row-1", newColumnFamily("test-family", "record-1", newColumn("testcol1", "value1"),
                newColumn("testcol2", "value2"), newColumn("testcol3", "value3")));
        assertEquals(row, fetchResult.rowResult.row);
    }

    @Test
    public void testFetchMissingRowByLocationId() throws Exception {
        try {
            Selector selector = new Selector().setLocationId("shard4/0");
            FetchResult fetchResult = new FetchResult();
            indexManager.fetchRow(TABLE, selector, fetchResult);
            fail("Should throw exception");
        } catch (BlurException e) {
        }
    }

    @Test
    public void testFetchRecordByLocationId() throws Exception {
        Selector selector = new Selector().setLocationId(SHARD_NAME + "/0").setRecordOnly(true);
        FetchResult fetchResult = new FetchResult();
        indexManager.fetchRow(TABLE, selector, fetchResult);
        assertNull(fetchResult.rowResult);
        assertNotNull(fetchResult.recordResult.record);

        assertEquals("row-1", fetchResult.recordResult.rowid);
        assertEquals("record-1", fetchResult.recordResult.recordid);
        assertEquals("test-family", fetchResult.recordResult.columnFamily);

        assertEquals(new TreeSet<Column>(Arrays.asList(newColumn("testcol1", "value1"),
                newColumn("testcol2", "value2"), newColumn("testcol3", "value3"))), fetchResult.recordResult.record);
    }

    @Test
    public void testFetchRowByRowId() throws Exception {
        Selector selector = new Selector().setRowId("row-1");
        FetchResult fetchResult = new FetchResult();
        indexManager.fetchRow(TABLE, selector, fetchResult);
        assertNotNull(fetchResult.rowResult.row);
        Row row = newRow("row-1", newColumnFamily("test-family", "record-1", newColumn("testcol1", "value1"),
                newColumn("testcol2", "value2"), newColumn("testcol3", "value3")));
        assertEquals(row, fetchResult.rowResult.row);
    }

    @Test
    public void testFetchRowByRecordIdOnly() throws Exception {
        Selector selector = new Selector().setRecordId("record-1");
        FetchResult fetchResult = new FetchResult();
        try {
            indexManager.fetchRow(TABLE, selector, fetchResult);
            fail("Invalid selector should throw exception.");
        } catch (BlurException e) {
            // do nothing, this is a pass
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testFetchRowByRecordIdOnlyNoRecordOnly() throws Exception {
        Selector selector = new Selector().setRowId("row-1").setRecordId("record-1");
        FetchResult fetchResult = new FetchResult();
        try {
            indexManager.fetchRow(TABLE, selector, fetchResult);
            fail("Invalid selector should throw exception.");
        } catch (BlurException e) {
            // do nothing, this is a pass
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testFetchRowByRecordId() throws Exception {
        Selector selector = new Selector().setRowId("row-1").setRecordId("record-1").setRecordOnly(true);
        FetchResult fetchResult = new FetchResult();
        indexManager.fetchRow(TABLE, selector, fetchResult);
        assertFalse(fetchResult.deleted);
        assertTrue(fetchResult.exists);
        assertEquals(TABLE, fetchResult.table);
        assertNull(fetchResult.rowResult);
        assertNotNull(fetchResult.recordResult);
        FetchRecordResult recordResult = fetchResult.recordResult;
        assertEquals("test-family", recordResult.columnFamily);
        assertEquals("record-1", recordResult.recordid);
        assertEquals("row-1", recordResult.rowid);

        assertEquals(new TreeSet<Column>(Arrays.asList(newColumn("testcol1", "value1"),
                newColumn("testcol2", "value2"), newColumn("testcol3", "value3"))), recordResult.record);

    }

    @Test
    public void testRecordFrequency() throws Exception {
        assertEquals(2, indexManager.recordFrequency(TABLE, "test-family", "testcol1", "value1"));
        assertEquals(0, indexManager.recordFrequency(TABLE, "test-family", "testcol1", "NO VALUE"));
    }

    @Test
    public void testSchema() throws Exception {
        Schema schema = indexManager.schema(TABLE);
        assertEquals(TABLE,schema.table);
        Map<String, Set<String>> columnFamilies = schema.columnFamilies;
        assertEquals(new TreeSet<String>(Arrays.asList("test-family")),new TreeSet<String>(columnFamilies.keySet()));
        assertEquals(new TreeSet<String>(Arrays.asList("testcol1","testcol2","testcol3")),new TreeSet<String>(columnFamilies.get("test-family")));
    }

    @Test
    public void testQuerySuperQueryTrue() throws Exception {
        BlurQuery blurQuery = new BlurQuery();
        blurQuery.queryStr = "test-family.testcol1:value1";
        blurQuery.superQueryOn = true;
        blurQuery.type = ScoreType.SUPER;
        blurQuery.fetch = 10;
        blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
        blurQuery.maxQueryTime = Long.MAX_VALUE;
        blurQuery.uuid = 1;

        BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
        assertEquals(iterable.getTotalResults(), 2);
        for (BlurResult result : iterable) {
            Selector selector = new Selector().setLocationId(result.getLocationId());
            FetchResult fetchResult = new FetchResult();
            indexManager.fetchRow(TABLE, selector, fetchResult);
            assertNotNull(fetchResult.rowResult);
            assertNull(fetchResult.recordResult);
        }

        assertFalse(indexManager.currentQueries(TABLE).isEmpty());
        Thread.sleep(2000);// wait for cleanup to fire
        assertTrue(indexManager.currentQueries(TABLE).isEmpty());
    }
    
    @Test
    public void testQuerySuperQueryTrueWithSelector() throws Exception {
        BlurQuery blurQuery = new BlurQuery();
        blurQuery.queryStr = "test-family.testcol1:value1";
        blurQuery.superQueryOn = true;
        blurQuery.type = ScoreType.SUPER;
        blurQuery.fetch = 10;
        blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
        blurQuery.maxQueryTime = Long.MAX_VALUE;
        blurQuery.uuid = 1;
        blurQuery.selector = new Selector();

        BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
        assertEquals(iterable.getTotalResults(), 2);
        for (BlurResult result : iterable) {
            assertNotNull(result.result.rowResult);
            assertNull(result.result.recordResult);
        }

        assertFalse(indexManager.currentQueries(TABLE).isEmpty());
        Thread.sleep(2000);// wait for cleanup to fire
        assertTrue(indexManager.currentQueries(TABLE).isEmpty());
    }
    
    @Test
    public void testQuerySuperQueryFalse() throws Exception {
        BlurQuery blurQuery = new BlurQuery();
        blurQuery.queryStr = "test-family.testcol1:value1";
        blurQuery.superQueryOn = false;
        blurQuery.fetch = 10;
        blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
        blurQuery.maxQueryTime = Long.MAX_VALUE;
        blurQuery.uuid = 1;

        BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
        assertEquals(iterable.getTotalResults(), 2);
        for (BlurResult result : iterable) {
            Selector selector = new Selector().setLocationId(result.getLocationId()).setRecordOnly(true);
            FetchResult fetchResult = new FetchResult();
            indexManager.fetchRow(TABLE, selector, fetchResult);
            assertNull(fetchResult.rowResult);
            assertNotNull(fetchResult.recordResult);
        }

        assertFalse(indexManager.currentQueries(TABLE).isEmpty());
        Thread.sleep(2000);// wait for cleanup to fire
        assertTrue(indexManager.currentQueries(TABLE).isEmpty());
    }
    
    @Test
    public void testQuerySuperQueryFalseWithSelector() throws Exception {
        BlurQuery blurQuery = new BlurQuery();
        blurQuery.queryStr = "test-family.testcol1:value1";
        blurQuery.superQueryOn = false;
        blurQuery.fetch = 10;
        blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
        blurQuery.maxQueryTime = Long.MAX_VALUE;
        blurQuery.uuid = 1;
        blurQuery.selector = new Selector();
        blurQuery.selector.setRecordOnly(true);

        BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
        assertEquals(iterable.getTotalResults(), 2);
        for (BlurResult result : iterable) {
            assertNull(result.result.rowResult);
            assertNotNull(result.result.recordResult);
        }

        assertFalse(indexManager.currentQueries(TABLE).isEmpty());
        Thread.sleep(2000);// wait for cleanup to fire
        assertTrue(indexManager.currentQueries(TABLE).isEmpty());
    }
    
    @Test
    public void testQueryWithFacets() throws Exception {
        BlurQuery blurQuery = new BlurQuery();
        blurQuery.queryStr = "test-family.testcol1:value1";
        blurQuery.superQueryOn = true;
        blurQuery.type = ScoreType.SUPER;
        blurQuery.fetch = 10;
        blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
        blurQuery.maxQueryTime = Long.MAX_VALUE;
        blurQuery.uuid = 1;
        blurQuery.facets = Arrays.asList(new Facet("test-family.testcol1:value1", Long.MAX_VALUE), 
                new Facet("test-family.testcol1:value-nohit", Long.MAX_VALUE));

        AtomicLongArray facetedCounts = new AtomicLongArray(2);
        BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, facetedCounts);
        assertEquals(iterable.getTotalResults(), 2);
        for (BlurResult result : iterable) {
            Selector selector = new Selector().setLocationId(result.getLocationId());
            FetchResult fetchResult = new FetchResult();
            indexManager.fetchRow(TABLE, selector, fetchResult);
            assertNotNull(fetchResult.rowResult);
            assertNull(fetchResult.recordResult);
        }

        assertEquals(2, facetedCounts.get(0));
        assertEquals(0, facetedCounts.get(1));

        assertFalse(indexManager.currentQueries(TABLE).isEmpty());
        Thread.sleep(2000);// wait for cleanup to fire
        assertTrue(indexManager.currentQueries(TABLE).isEmpty());
    }

    @Test
    public void testTerms() throws Exception {
        List<String> terms = indexManager.terms(TABLE, "test-family", "testcol1", "", (short) 100);
        assertEquals(Arrays.asList("value1","value4","value7"), terms);
    }

    @Test
    public void testMutationReplaceRow() throws Exception {
        List<RowMutation> mutations = newRowMutations(
                newRowMutation("row-4", 
                        newRecordMutation("test-family", "record-4", 
                                newColumn("testcol1", "value2"),
                                newColumn("testcol2", "value3"), 
                                newColumn("testcol3", "value4"))));
        indexManager.mutate(TABLE, mutations);
        
        Selector selector = new Selector().setRowId("row-4");
        FetchResult fetchResult = new FetchResult();
        indexManager.fetchRow(TABLE, selector, fetchResult);
        assertNotNull(fetchResult.rowResult.row);
        Row row = newRow("row-4", 
                    newColumnFamily("test-family", "record-4", 
                            newColumn("testcol1", "value2"),
                            newColumn("testcol2", "value3"), 
                            newColumn("testcol3", "value4")));
        assertEquals(row, fetchResult.rowResult.row);
    }

}
