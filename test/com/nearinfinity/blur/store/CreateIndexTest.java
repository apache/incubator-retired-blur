package com.nearinfinity.blur.store;

import java.util.Random;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.store.dao.hbase.HbaseDao;

public class CreateIndexTest {

	private static Random random = new Random();

	public static void main(String[] args) throws Exception {
//		BlurDirectory directory = new BlurDirectory(new CassandraDao("Keyspace1", "Standard1", "testing", ConsistencyLevel.ONE, 10, "localhost", 9160));
//		FSDirectory directory = FSDirectory.open(new File("./index"));
		BlurDirectory directory = new BlurDirectory(new HbaseDao("t1", "f1", "testing"));
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		final IndexWriter indexWriter = new IndexWriter(directory, analyzer, MaxFieldLength.UNLIMITED);
		indexWriter.setUseCompoundFile(false);
		long os = System.currentTimeMillis();
		for (int y= 0; y < 1000; y++) {
			long is = System.currentTimeMillis();
			int count = 0;
			int max = 1000;
			for (int i = 0; i < 100000; i++) {
				if (count >= max) {
					System.out.println("Pass [" + y + 
							"] Total [" + i + "]");
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
		}
		indexWriter.optimize();
		System.out.println("optimize time [" + (System.currentTimeMillis() - os) + "]");
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
