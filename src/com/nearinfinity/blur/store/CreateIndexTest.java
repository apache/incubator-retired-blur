package com.nearinfinity.blur.store;

import java.util.Random;
import java.util.UUID;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.util.Version;

public class CreateIndexTest {

	private static Random random = new Random();

	public static void main(String[] args) throws Exception {
		CassandraDirectory directory = new CassandraDirectory("Keyspace1", "Standard1", "testing",
				ConsistencyLevel.ONE, 10, "localhost", 9160);
//		FSDirectory directory = FSDirectory.open(new File("./index"));
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		final IndexWriter indexWriter = new IndexWriter(directory, analyzer, MaxFieldLength.UNLIMITED);
		indexWriter.setUseCompoundFile(false);
		long is = System.currentTimeMillis();
		int count = 0;
		int max = 10000;
		for (int i = 0; i < 10000000; i++) {
			if (count >= max) {
				System.out.println("Total [" + i + "]");
				count = 0;
			}
			try {
				indexWriter.addDocument(genDoc());
			} catch (IllegalStateException e) {
				e.printStackTrace();
				throw new RuntimeException();
			}
			count++;
		}
		System.out.println("indexing time [" + (System.currentTimeMillis() - is) + "]");
		indexWriter.commit();
		indexWriter.optimize();
		System.out.println("optimize time [" + (System.currentTimeMillis() - is) + "]");
		indexWriter.close();

	}

	private static Document genDoc() {
		Document document = new Document();
		document.add(new Field("test", "test", Store.YES, Index.ANALYZED_NO_NORMS));
		document.add(new Field("id", UUID.randomUUID().toString(), Store.YES, Index.ANALYZED_NO_NORMS));
		document.add(new Field("random10", Integer.toString(random.nextInt(10)), Store.YES, Index.ANALYZED_NO_NORMS));
		document.add(new Field("random100", Integer.toString(random.nextInt(100)), Store.YES, Index.ANALYZED_NO_NORMS));
		document.add(new Field("random1000", Integer.toString(random.nextInt(1000)), Store.YES, Index.ANALYZED_NO_NORMS));
		document.add(new Field("random10000", Integer.toString(random.nextInt(10000)), Store.YES, Index.ANALYZED_NO_NORMS));
		document.add(new Field("random100000", Integer.toString(random.nextInt(100000)), Store.YES, Index.ANALYZED_NO_NORMS));
		document.add(new Field("random1000000", Integer.toString(random.nextInt(1000000)), Store.YES, Index.ANALYZED_NO_NORMS));
		return document;
	}

}
