package com.nearinfinity.blur.manager;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Document;
import com.nearinfinity.blur.thrift.generated.DocumentFamily;
import com.nearinfinity.blur.thrift.generated.Mutation;
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

	public void testIndexManager() throws IOException, BlurException {
		IndexManager indexManager = new IndexManager();
		Mutation mutation = new Mutation();
		mutation.id = "1";
		DocumentFamily elem = new DocumentFamily();
		elem.setName("person");
		Document doc = new Document();
		doc.id = "1";
		doc.putToValues("name", "aaron");
		elem.addToDocuments(doc);
		mutation.addToDocumentFamilies(elem);
		indexManager.update("test",mutation);
		
		Map<String, IndexReader> indexReaders = indexManager.getIndexReaders("test");
		int total = 0;
		for (Entry<String, IndexReader> entry : indexReaders.entrySet()) {
			total += entry.getValue().numDocs();
		}
		assertTrue(total > 0);
	}


}
