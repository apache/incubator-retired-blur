package com.nearinfinity.blur.manager;

import static com.nearinfinity.blur.thrift.ThriftUtil.newColumn;
import static com.nearinfinity.blur.thrift.ThriftUtil.newColumnFamily;
import static com.nearinfinity.blur.thrift.ThriftUtil.newRow;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.index.IndexReader;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.manager.util.MeleFactory;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.mele.Mele;

public class IndexManagerTest {
	
	private Mele mele;

	@Before
    public void setUp() throws Exception {
		rm(new File("target/test-tmp"));
		MeleFactory.setup(new LocalHdfsMeleConfiguration());
        mele = MeleFactory.getInstance();
		mele.createDirectoryCluster("test");
		mele.createDirectory("test", "s1");
		mele.createDirectory("test", "s2");
		mele.createDirectory("test", "s3");
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
	public void testIndexManager() throws IOException, BlurException, MissingShardException {
		IndexManager indexManager = new IndexManager();
		Row row = newRow("1", newColumnFamily("person", "1", newColumn("name", "aaron")));
		indexManager.replaceRow("test",row);
		
		Map<String, IndexReader> indexReaders = indexManager.getIndexReaders("test");
		int total = 0;
		for (Entry<String, IndexReader> entry : indexReaders.entrySet()) {
			total += entry.getValue().numDocs();
		}
		assertTrue(total > 0);
		
		Row r = indexManager.fetchRow("test", "1");
		assertEquals(row,r);
		
		indexManager.close();
	}


}
