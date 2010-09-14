package com.nearinfinity.blur.manager;

import static com.nearinfinity.blur.manager.IndexManagerTest.rm;
import static com.nearinfinity.blur.thrift.ThriftUtil.newColumn;
import static com.nearinfinity.blur.thrift.ThriftUtil.newColumnFamily;
import static com.nearinfinity.blur.thrift.ThriftUtil.newRow;
import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.mele.Mele;

public class ComplexIndexManagerTest {

    private static final String SHARD_NAME = "shard";
    private static final String TABLE_NAME = "complex-test";
	private static Mele mele;
	private static IndexManager indexManager;
    private static ArrayList<Row> rows;

    @BeforeClass
    public static void setUpOnce() throws Exception {
	    String pathname = "target/test-tmp-complex";
        rm(new File(pathname));
	    LocalHdfsMeleConfiguration configuration = new LocalHdfsMeleConfiguration(pathname);
        mele = new Mele(configuration);
    	mele.createDirectoryCluster(TABLE_NAME);
    	mele.createDirectory(TABLE_NAME, SHARD_NAME);
    	
    	System.out.println(mele.listDirectories(TABLE_NAME));
    	System.out.println(mele.listLocalDirectories(TABLE_NAME));
    	
    	indexManager = new IndexManager(mele);
    	populate();
	}
    
    @AfterClass
    public static void oneTimeTearDown() throws InterruptedException {
        indexManager.close();
    }

	private static void populate() throws IOException, BlurException, MissingShardException {
	    rows = new ArrayList<Row>();
	    rows.add(newRow("1000", 
                newColumnFamily("person", "1234", 
                        newColumn("private","true"),
                        newColumn("name", "aaron mccurry", "aaron patrick mccurry", "aaron p mccurry"),
                        newColumn("gender", "male"),
                        newColumn("dob","19781008","19781000")),
                newColumnFamily("address","5678",
                        newColumn("private","true"),
                        newColumn("street","155 johndoe","155 johndoe Court"),
                        newColumn("city","thecity"))));
	    rows.add(newRow("2000", 
	            newColumnFamily("person", "8395", 
                        newColumn("private","true"),
                        newColumn("name", "johnathon doe", "johnathon j doe"),
                        newColumn("gender", "male"),
                        newColumn("dob","19810902","19810900")),
                newColumnFamily("address","2816",
                        newColumn("private","true"),
                        newColumn("street","155 1st","155 1st street"),
                        newColumn("city","thecity"))));
	    rows.add(newRow("3000", 
	            newColumnFamily("person", "6239", 
                        newColumn("private","false"),
                        newColumn("name", "jane doe", "jane j doe"),
                        newColumn("gender", "female"),
                        newColumn("dob","19560000","19560723")),
                newColumnFamily("address","9173",
                        newColumn("private","false"),
                        newColumn("street","12347 27th steet apt. 1234-c"),
                        newColumn("city","thecity"))));
	    
	    for (Row r : rows) {
	        indexManager.replaceRow(TABLE_NAME, r);
	    }
	}
	
	@Test
	public void testSimpleSearchWithFetch1() throws IOException, BlurException, MissingShardException {
		HitsIterable hits = indexManager.search(TABLE_NAME, "person.name:aaron", true, ScoreType.SUPER, 
				null, null, Long.MAX_VALUE, Long.MAX_VALUE);
		assertEquals(1, hits.getTotalHits());
		Hit hit = hits.iterator().next();
		assertEquals("1000", hit.id);
		assertEquals(rows.get(0), indexManager.fetchRow(TABLE_NAME, hit.id));
	}
	
	@Test
    public void testSimpleSearchWithFetch2() throws IOException, BlurException, MissingShardException {
	    HitsIterable hitsNoFilter = indexManager.search(TABLE_NAME, "person.name:johnathon", true, ScoreType.SUPER, 
                null, null, Long.MAX_VALUE, Long.MAX_VALUE);
        assertEquals(1, hitsNoFilter.getTotalHits());
        Hit hitNoFilter = hitsNoFilter.iterator().next();
        assertEquals("2000", hitNoFilter.id);
        assertEquals(rows.get(1), indexManager.fetchRow(TABLE_NAME, hitNoFilter.id));
    }
	
	@Test
	public void testSimpleSearchWithFilterAndFetchWithFalse() throws IOException, BlurException, MissingShardException {
	    HitsIterable hitsAfterFilterFalse = indexManager.search(TABLE_NAME, "person.name:johnathon", true, ScoreType.SUPER, 
                null, "address.private:false person.private:false", Long.MAX_VALUE, Long.MAX_VALUE);
        assertEquals(0, hitsAfterFilterFalse.getTotalHits());
    }
	
	@Test
    public void testSimpleSearchWithFilterAndFetchWithTrue() throws IOException, BlurException, MissingShardException {
	    HitsIterable hitsAfterFilterTrue = indexManager.search(TABLE_NAME, "person.name:johnathon", true, ScoreType.SUPER, 
                null, "address.private:true person.private:true", Long.MAX_VALUE, Long.MAX_VALUE);
        assertEquals(1, hitsAfterFilterTrue.getTotalHits());
    }
	
}
