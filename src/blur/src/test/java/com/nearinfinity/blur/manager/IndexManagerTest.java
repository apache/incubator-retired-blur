package com.nearinfinity.blur.manager;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.index.IndexReader;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SuperColumn;
import com.nearinfinity.mele.Mele;

public class IndexManagerTest {
	
	private Mele mele;

	@Before
    public void setUp() throws Exception {
		rm(new File("target/test-tmp"));
		mele = Mele.getMele(new LocalHdfsMeleConfiguration());
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
		Row row = new Row();
		row.id="1";
		SuperColumn sc = new SuperColumn();
		sc.family = "person";
		sc.id = "1";
		Column col = new Column();
		col.name = "name";
		col.addToValues("aaron");
		sc.addToColumns(col);
		row.addToSuperColumns(sc);
		
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
