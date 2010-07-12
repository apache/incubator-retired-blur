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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.store.dao.hbase.HbaseDao;
import com.nearinfinity.blur.store.policy.ZookeeperIndexDeletionPolicy;

public class IndexWriterProcess {

	private static Random random = new Random();

	public static void main(String[] args) throws Exception {
		final ZooKeeper zk = new ZooKeeper("localhost", 3000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
		
		final String indexRefPath = "/blur/refs/testing";
//		FSDirectory dir = FSDirectory.open(new File("./index"));
		BlurDirectory dir = new BlurDirectory(new HbaseDao("t1", "f1", "testing"));
		final ZookeeperWrapperDirectory directory = new ZookeeperWrapperDirectory(zk, dir, indexRefPath);
		final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		IndexWriter indexWriter = new IndexWriter(directory, analyzer, new ZookeeperIndexDeletionPolicy(zk, indexRefPath), MaxFieldLength.UNLIMITED);
		indexWriter.setUseCompoundFile(false);
		while (true) {
			for (int i = 0; i < 10000; i++) {
				indexWriter.addDocument(genDoc());
			}
			indexWriter.commit();
			Thread.sleep(10);
		}
//		indexWriter.close();

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
