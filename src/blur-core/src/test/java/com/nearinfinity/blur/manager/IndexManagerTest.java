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
import static com.nearinfinity.blur.utils.BlurUtil.newRecordMutation;
import static com.nearinfinity.blur.utils.BlurUtil.newRowMutation;
import static com.nearinfinity.blur.utils.BlurUtil.newRowMutations;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.BlurShardName;
import com.nearinfinity.blur.manager.indexserver.LocalIndexServer;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.utils.BlurConstants;

public class IndexManagerTest {

    private static final String TABLE = "table";
    private IndexServer server;
    private IndexManager indexManager;

    @Before
    public void setUp() throws BlurException, IOException {
//        server = new LocalIndexServer(new File("./test-indexes/test1"));
        File file = new File("./tmp/indexer-manager-test");
        new File(new File(file,TABLE),BlurShardName.getShardName(BlurConstants.SHARD_PREFIX, 0)).mkdirs();
        server = new LocalIndexServer(file);
        indexManager = new IndexManager();
        indexManager.setStatusCleanupTimerDelay(2000);
        indexManager.setIndexServer(server);
        indexManager.init();
    }
    
    private void setupData() throws BlurException, IOException {
        List<RowMutation> mutations = newRowMutations(
                newRowMutation("row-1",
                        newRecordMutation("test-family","record-1",
                                newColumn("testcol1", "value1"),
                                newColumn("testcol2", "value2"),
                                newColumn("testcol3", "value3"))
                        ),
                newRowMutation("row-2",
                        newRecordMutation("test-family","record-2",
                                newColumn("testcol1", "value4"),
                                newColumn("testcol2", "value5"),
                                newColumn("testcol3", "value6"))
                        ),
                newRowMutation("row-3",
                        newRecordMutation("test-family","record-3",
                                newColumn("testcol1", "value7"),
                                newColumn("testcol2", "value8"),
                                newColumn("testcol3", "value9"))
                        ));
        indexManager.mutate(TABLE, mutations);
    }

    @After
    public void tearDown() throws InterruptedException {
        indexManager.close();
    }
    
    @Test
    public void testSetupTestData() throws BlurException, IOException {
        setupData();
    }

//    @Test
//    public void testFetchRow1() throws Exception {
//        Selector selector = new Selector().setLocationId("shard1/0");
//        FetchResult fetchResult = new FetchResult();
//        indexManager.fetchRow(TABLE, selector, fetchResult);
//        assertNotNull(fetchResult.row);
//    }
//    
//    @Test
//    public void testFetchRow2() throws Exception {
//        try {
//            Selector selector = new Selector().setLocationId("shard4/0");
//            FetchResult fetchResult = new FetchResult();
//            indexManager.fetchRow(TABLE, selector, fetchResult);
//            fail("Should throw exception");
//        } catch (BlurException e) {
//        }
//    }
//    
//    @Test
//    public void testFetchRecord1() throws Exception {
//        Selector selector = new Selector().setLocationId("shard1/0").setRecordOnly(true);
//        FetchResult fetchResult = new FetchResult();
//        indexManager.fetchRow(TABLE, selector, fetchResult);
//        assertNull(fetchResult.row);
//        assertNotNull(fetchResult.record);
//        System.out.println(fetchResult.record);
//    }
//    
//    @Test
//    public void testQuery() throws Exception {
//        BlurQuery blurQuery = new BlurQuery();
//        blurQuery.queryStr = "test-fam.name:value";
//        blurQuery.superQueryOn = true;
//        blurQuery.type = ScoreType.SUPER;
//        blurQuery.fetch = 10;
//        blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
//        blurQuery.maxQueryTime = Long.MAX_VALUE;
//        blurQuery.uuid = 1;
//        
//        BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
//        assertEquals(iterable.getTotalResults(),2);
//        for (BlurResult result : iterable) {
//            Selector selector = new Selector().setLocationId(result.getLocationId());
//            FetchResult fetchResult = new FetchResult();
//            indexManager.fetchRow(TABLE, selector, fetchResult);
//            System.out.println(fetchResult.getRow());
//        }
//        
//        assertFalse(indexManager.currentQueries(TABLE).isEmpty());
//        Thread.sleep(5000);//wait for cleanup to fire
//        assertTrue(indexManager.currentQueries(TABLE).isEmpty());
//    }
//    
//    @Test
//    public void testQueryWithFacets() throws Exception {
//        BlurQuery blurQuery = new BlurQuery();
//        blurQuery.queryStr = "test-fam.name:value";
//        blurQuery.superQueryOn = true;
//        blurQuery.type = ScoreType.SUPER;
//        blurQuery.fetch = 10;
//        blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
//        blurQuery.maxQueryTime = Long.MAX_VALUE;
//        blurQuery.uuid = 1;
//        blurQuery.facets = Arrays.asList(new Facet("test-fam.name:value", Long.MAX_VALUE),new Facet("test-fam.name:value-nohit", Long.MAX_VALUE));
//        
//        AtomicLongArray facetedCounts = new AtomicLongArray(2);
//        BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, facetedCounts);
//        assertEquals(iterable.getTotalResults(),2);
//        for (BlurResult result : iterable) {
//            Selector selector = new Selector().setLocationId(result.getLocationId());
//            FetchResult fetchResult = new FetchResult();
//            indexManager.fetchRow(TABLE, selector, fetchResult);
//            System.out.println(fetchResult.getRow());
//        }
//        
//        assertEquals(2, facetedCounts.get(0));
//        assertEquals(0, facetedCounts.get(1));
//        
//        assertFalse(indexManager.currentQueries(TABLE).isEmpty());
//        Thread.sleep(5000);//wait for cleanup to fire
//        assertTrue(indexManager.currentQueries(TABLE).isEmpty());
//    }
//    
//    @Test
//    public void testTerms() throws Exception {
//        List<String> terms = indexManager.terms(TABLE, "test-fam", "name", "", (short) 100);
//        assertEquals(Arrays.asList("value"),terms);
//    }
//    
//    @Test
//    public void testRecordFrequency() throws Exception {
//        assertEquals(2,indexManager.recordFrequency(TABLE, "test-fam", "name", "value"));
//        assertEquals(0,indexManager.recordFrequency(TABLE, "test-fam", "name", "value2"));
//    }
//    
//    @Test
//    public void testSchema() throws Exception {
//        Schema schema = indexManager.schema(TABLE);
//        System.out.println(schema);
//    }
//    
//    @Test
//    public void testRemoveRow() throws Exception {
//        try {
//            indexManager.removeRow(null, null);
//            fail("not implemented.");
//        } catch (Exception e) {
//        }
//    }
//    
//    @Test
//    public void testReplaceRow() throws Exception {
//        try {
//            indexManager.replaceRow(null, null);
//            fail("not implemented.");
//        } catch (Exception e) {
//        }
//    }

}
