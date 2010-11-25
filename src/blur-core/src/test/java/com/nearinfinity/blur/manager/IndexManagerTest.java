package com.nearinfinity.blur.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.manager.local.LocalIndexServer;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.Selector;

public class IndexManagerTest {

    private IndexServer server;
    private IndexManager indexManager;

    // IndexWriter writer = new IndexWriter(FSDirectory.open(new File("./test-indexes/test1/table/shard2")), new
    // StandardAnalyzer(Version.LUCENE_30), MaxFieldLength.UNLIMITED);
    // writer.setSimilarity(new FairSimilarity());
    // Row row = new Row().setId("2");
    // ColumnFamily family = new ColumnFamily().setFamily("test-fam");
    // Set<Column> val = new HashSet<Column>();
    // Column column = new Column().setName("name");
    // column.addToValues("value");
    // val.add(column);
    // family.putToColumns("id2", val);
    // row.addToColumnFamilies(family);
    // IndexManager.replace(writer, row);
    // writer.close();

    @Before
    public void setUp() {
        server = new LocalIndexServer(new File("./test-indexes/test1"));
        indexManager = new IndexManager();
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
        } catch (MissingShardException e) {
        }
    }
    
    @Test
    public void testFetchRecord1() throws Exception {
        Selector selector = new Selector().setLocationId("shard1/0").setRecordOnly(true);
        FetchResult fetchResult = new FetchResult();
        indexManager.fetchRow("table", selector, fetchResult);
        assertNull(fetchResult.row);
        assertNotNull(fetchResult.record);
    }
    
    @Test
    public void testSearch() throws Exception {
        String query = "test-fam.name:value";
        HitsIterable iterable = indexManager.search("table", query, true, ScoreType.SUPER, null, null, 10, Long.MAX_VALUE);
        assertEquals(iterable.getTotalHits(),2);
        for (Hit hit : iterable) {
            System.out.println(hit);
        }
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
