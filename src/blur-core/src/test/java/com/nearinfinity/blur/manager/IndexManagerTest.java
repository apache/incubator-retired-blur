package com.nearinfinity.blur.manager;

import static com.nearinfinity.blur.utils.ThriftUtil.newColumn;
import static com.nearinfinity.blur.utils.ThriftUtil.newColumnFamily;
import static com.nearinfinity.blur.utils.ThriftUtil.newRow;
import static com.nearinfinity.blur.utils.ThriftUtil.newSelector;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.index.IndexReader;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.mele.Mele;
import com.nearinfinity.mele.MeleBase;
import com.nearinfinity.mele.store.noreplication.NoRepMeleDirectoryFactory;
import com.nearinfinity.mele.zookeeper.NoOpWatcher;

public class IndexManagerTest {
	
	private Mele mele;
    private ZooKeeper zooKeeper;

	@Before
    public void setUp() throws Exception {
	    String pathname = "target/test-tmp-index-manager";
        rm(new File(pathname));
        LocalHdfsMeleConfiguration configuration = new LocalHdfsMeleConfiguration(pathname);
        zooKeeper = new ZooKeeper(configuration.getZooKeeperConnectionString(), 
                configuration.getZooKeeperSessionTimeout(), new NoOpWatcher());
        mele = new MeleBase(new NoRepMeleDirectoryFactory(), configuration, zooKeeper);
		mele.createDirectoryCluster("test");
		mele.createDirectory("test", "s1");
		mele.createDirectory("test", "s2");
		mele.createDirectory("test", "s3");
	}
	
	public void tearDown() throws Exception {
	    zooKeeper.close();
	}

	public static void rm(File file) {
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				rm(f);
			}
		}
		file.delete();
	}

	@Test
	public void testIndexManager() throws IOException, BlurException, MissingShardException, InterruptedException {
		IndexManager indexManager = new IndexManager(mele);
		Row row = newRow("1", newColumnFamily("person", "1", newColumn("name", "aaron")));
		indexManager.replaceRow("test",row);
		
		Map<String, IndexReader> indexReaders = indexManager.getIndexReaders("test");
		int total = 0;
		for (Entry<String, IndexReader> entry : indexReaders.entrySet()) {
			total += entry.getValue().numDocs();
		}
		assertTrue(total > 0);
		
		Row r = indexManager.fetchRow("test", newSelector("1"));
		assertEquals(row,r);
		
		indexManager.close();
	}


}
