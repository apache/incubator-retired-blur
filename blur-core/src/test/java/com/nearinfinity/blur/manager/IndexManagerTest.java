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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.manager.hits.BlurResultIterable;
import com.nearinfinity.blur.manager.indexserver.LocalIndexServer;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.Facet;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.Selector;

public class IndexManagerTest {

    private IndexServer server;
    private IndexManager indexManager;

//    @BeforeClass
//    public static void once() throws CorruptIndexException, LockObtainFailedException, IOException {
//        IndexWriter writer = new IndexWriter(FSDirectory.open(new File("./test-indexes/test1/table/shard1")), new
//                StandardAnalyzer(Version.LUCENE_30), MaxFieldLength.UNLIMITED);
//                writer.setSimilarity(new FairSimilarity());
//                writer.setUseCompoundFile(false);
//                Row row = new Row().setId("2");
//                ColumnFamily family = new ColumnFamily().setFamily("test-fam");
//                Set<Column> val = new HashSet<Column>();
//                Column column = new Column().setName("name");
//                column.addToValues("value");
//                val.add(column);
//                family.putToColumns("id2", val);
//                row.addToColumnFamilies(family);
//                RowIndexWriter indexWriter = new RowIndexWriter(writer, new BlurAnalyzer(new StandardAnalyzer(Version.LUCENE_30), ""));
//                indexWriter.replace(row);
//                writer.close();
//    }
    
    @Before
    public void setUp() {
        server = new LocalIndexServer(new File("./test-indexes/test1"));
        indexManager = new IndexManager();
        indexManager.setSearchStatusCleanupTimerDelay(2000);
        indexManager.setIndexServer(server);
        indexManager.init();
    }
    
    @After
    public void tearDown() throws InterruptedException {
        indexManager.close();
    }

    @Test
    public void testFetchRow1() throws Exception {
        Selector selector = new Selector().setLocationId("shard1/0");
        FetchResult fetchResult = new FetchResult();
        indexManager.fetchRow("table", selector, fetchResult);
        assertNotNull(fetchResult.row);
    }
    
    @Test
    public void testFetchRow2() throws Exception {
        try {
            Selector selector = new Selector().setLocationId("shard4/0");
            FetchResult fetchResult = new FetchResult();
            indexManager.fetchRow("table", selector, fetchResult);
            fail("Should throw exception");
        } catch (BlurException e) {
        }
    }
    
    @Test
    public void testFetchRecord1() throws Exception {
        Selector selector = new Selector().setLocationId("shard1/0").setRecordOnly(true);
        FetchResult fetchResult = new FetchResult();
        indexManager.fetchRow("table", selector, fetchResult);
        assertNull(fetchResult.row);
        assertNotNull(fetchResult.record);
        System.out.println(fetchResult.record);
    }
    
    @Test
    public void testSearch() throws Exception {
        BlurQuery searchQuery = new BlurQuery();
        searchQuery.queryStr = "test-fam.name:value";
        searchQuery.superQueryOn = true;
        searchQuery.type = ScoreType.SUPER;
        searchQuery.fetch = 10;
        searchQuery.minimumNumberOfResults = Long.MAX_VALUE;
        searchQuery.maxQueryTime = Long.MAX_VALUE;
        searchQuery.uuid = 1;
        
        BlurResultIterable iterable = indexManager.query("table", searchQuery, null);
        assertEquals(iterable.getTotalResults(),2);
        for (BlurResult hit : iterable) {
            Selector selector = new Selector().setLocationId(hit.getLocationId());
            FetchResult fetchResult = new FetchResult();
            indexManager.fetchRow("table", selector, fetchResult);
            System.out.println(fetchResult.getRow());
        }
        
        assertFalse(indexManager.currentSearches("table").isEmpty());
        Thread.sleep(5000);//wait for cleanup to fire
        assertTrue(indexManager.currentSearches("table").isEmpty());
    }
    
    @Test
    public void testSearchWithFacets() throws Exception {
        BlurQuery searchQuery = new BlurQuery();
        searchQuery.queryStr = "test-fam.name:value";
        searchQuery.superQueryOn = true;
        searchQuery.type = ScoreType.SUPER;
        searchQuery.fetch = 10;
        searchQuery.minimumNumberOfResults = Long.MAX_VALUE;
        searchQuery.maxQueryTime = Long.MAX_VALUE;
        searchQuery.uuid = 1;
        searchQuery.facets = Arrays.asList(new Facet("test-fam.name:value", Long.MAX_VALUE),new Facet("test-fam.name:value-nohit", Long.MAX_VALUE));
        
        AtomicLongArray facetedCounts = new AtomicLongArray(2);
        BlurResultIterable iterable = indexManager.query("table", searchQuery, facetedCounts);
        assertEquals(iterable.getTotalResults(),2);
        for (BlurResult hit : iterable) {
            Selector selector = new Selector().setLocationId(hit.getLocationId());
            FetchResult fetchResult = new FetchResult();
            indexManager.fetchRow("table", selector, fetchResult);
            System.out.println(fetchResult.getRow());
        }
        
        assertEquals(2, facetedCounts.get(0));
        assertEquals(0, facetedCounts.get(1));
        
        assertFalse(indexManager.currentSearches("table").isEmpty());
        Thread.sleep(5000);//wait for cleanup to fire
        assertTrue(indexManager.currentSearches("table").isEmpty());
    }
    
    @Test
    public void testTerms() throws Exception {
        List<String> terms = indexManager.terms("table", "test-fam", "name", "", (short) 100);
        assertEquals(Arrays.asList("value"),terms);
    }
    
    @Test
    public void testRecordFrequency() throws Exception {
        assertEquals(2,indexManager.recordFrequency("table", "test-fam", "name", "value"));
        assertEquals(0,indexManager.recordFrequency("table", "test-fam", "name", "value2"));
    }
    
    @Test
    public void testSchema() throws Exception {
        Schema schema = indexManager.schema("table");
        System.out.println(schema);
    }
    
    @Test
    public void testRemoveRow() throws Exception {
        try {
            indexManager.removeRow(null, null);
            fail("not implemented.");
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testReplaceRow() throws Exception {
        try {
            indexManager.replaceRow(null, null);
            fail("not implemented.");
        } catch (Exception e) {
        }
    }

}
