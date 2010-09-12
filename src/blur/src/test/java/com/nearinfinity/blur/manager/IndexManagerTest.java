package com.nearinfinity.blur.manager;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SuperColumn;
import com.nearinfinity.blur.thrift.generated.SuperColumnFamily;
import com.nearinfinity.mele.Mele;

public class IndexManagerTest extends TestCase {
	
	private Mele mele;

	@Override
	protected void setUp() throws Exception {
		rm(new File("./tmp"));
		mele = Mele.getMele(new LocalHdfsMeleConfiguration());
		mele.createDirectoryCluster("test");
		mele.createDirectory("test", "s1");
		mele.createDirectory("test", "s2");
		mele.createDirectory("test", "s3");
	}

	private void rm(File file) {
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				rm(f);
			}
		}
		file.delete();
	}

	public void testIndexManager() throws IOException, BlurException, MissingShardException {
		IndexManager indexManager = new IndexManager();
		Row row = new Row();
		row.id="1";
		SuperColumnFamily scf = new SuperColumnFamily();
		scf.name = "person";
		SuperColumn sc = new SuperColumn();
		sc.id = "1";
		Column col = new Column();
		col.name = "name";
		col.values = Arrays.asList("aaron");
		sc.columns = new HashMap<String, Column>();
		sc.columns.put("name", col);
		scf.superColumns = new HashMap<String, SuperColumn>();
		scf.superColumns.put("1", sc);
		row.superColumnFamilies = new TreeMap<String, SuperColumnFamily>();
		row.superColumnFamilies.put("person", scf);
		indexManager.replaceRow("test",row);
		
		Map<String, IndexReader> indexReaders = indexManager.getIndexReaders("test");
		int total = 0;
		for (Entry<String, IndexReader> entry : indexReaders.entrySet()) {
			total += entry.getValue().numDocs();
		}
		assertTrue(total > 0);
		
		Row r = indexManager.fetchRow("test", "1");
		assertEquals(row,r);
	}


}
