package com.nearinfinity.blur.store;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.lucene.store.ZookeeperWrapperDirectory;
import com.nearinfinity.blur.lucene.store.policy.ZookeeperIndexDeletionPolicy;

public class CreateIndexTest {

	private static Random random = new Random();

	public static void main(String[] args) throws Exception {
		final ZooKeeper zk = new ZooKeeper("localhost", 3000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
		
		final String indexRefPath = "/blur/refs/testing";
		FSDirectory dir = FSDirectory.open(new File("./index"));
		final ZookeeperWrapperDirectory directory = new ZookeeperWrapperDirectory(dir, indexRefPath);
		final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						IndexWriter indexWriter = new IndexWriter(directory, analyzer, new ZookeeperIndexDeletionPolicy(zk, indexRefPath), MaxFieldLength.UNLIMITED);
						indexWriter.setUseCompoundFile(false);
						for (int i = 0; i < 1000; i++) {
							indexWriter.addDocument(genDoc());
						}
						indexWriter.commit();
						indexWriter.close();
						Thread.sleep(10);
					}
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}
			}
		}).start();
		
		IndexReader reader = IndexReader.open(directory);
		long nextCheckTime = System.currentTimeMillis() + 30000;
		while (true) {
			if (nextCheckTime < System.currentTimeMillis() && !reader.isCurrent()) {
				System.out.println("Reopening reader...");
				IndexReader newReader = IndexReader.open(directory);
				System.out.println("Closing old reader...");
				reader.close();
				List<String> listOfReferencedFiles = ZookeeperIndexDeletionPolicy.getListOfReferencedFiles(zk, indexRefPath);
				System.out.println(listOfReferencedFiles);
				reader = null;
				reader = newReader;
				nextCheckTime = System.currentTimeMillis() + 30000;
			}
			IndexSearcher searcher = new IndexSearcher(reader);
			TopDocs topDocs = searcher.search(new TermQuery(new Term("test","test")), 10);
			System.out.println("found [" + topDocs.totalHits + "]");
			Thread.sleep(100);
		}

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
