package com.nearinfinity.blur.lucene.store.dao.cassandra;

import java.util.Arrays;
import java.util.UUID;

import junit.framework.TestCase;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.lucene.store.BlurBaseDirectory;
import com.nearinfinity.blur.lucene.store.dao.cassandra.thrift.CassandraStore;

public class CassandraDirectoryTest extends TestCase {
	
	public void testDirectoryByCreatingAnIndex() throws Exception {
		CassandraStore store = new CassandraStore("Keyspace1", "Standard1", "testdir"+UUID.randomUUID().toString(), ConsistencyLevel.ALL, 1, "192.168.1.202", 9160);
		BlurBaseDirectory directory = new BlurBaseDirectory(store);
		generateIndex(directory);
	}

	private void generateIndex(Directory directory) throws Exception {
		System.out.println("Creating Indexer");
		System.out.println(Arrays.asList(directory.listAll()));
		IndexWriter indexWriter = new IndexWriter(directory,new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		System.out.println("Indexing");
		for (int i = 0; i < 100000; i++) {
			indexWriter.addDocument(genDoc());
		}
		System.out.println("Optimizing");
		indexWriter.optimize();
		indexWriter.close();
	}

	private Document genDoc() {
		Document document = new Document();
		document.add(new Field("id",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
		document.add(new Field("test","test",Store.YES,Index.ANALYZED_NO_NORMS));
		return document;
	}

}
