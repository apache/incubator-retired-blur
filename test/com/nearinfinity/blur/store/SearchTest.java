package com.nearinfinity.blur.store;

import java.io.File;
import java.util.Random;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.lucene.store.ZookeeperWrapperDirectory;

public class SearchTest {
	
	private static Random random = new Random();

	public static void main(String[] args) throws Exception {
		ZooKeeper zk = new ZooKeeper("localhost", 3000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
		String indexRefPath = "/blur/refs/testing";
//		BlurDirectory dir = new BlurDirectory(new HbaseDao("t1", "f1", "testing"));
		FSDirectory dir = FSDirectory.open(new File("./index"));
		ZookeeperWrapperDirectory directory = new ZookeeperWrapperDirectory(zk, dir, indexRefPath);
//		BlurDirectory directory = new BlurDirectory(new HbaseDao("t1", "f1", "testing"));
//		BlurDirectory directory = new BlurDirectory(new CassandraDao("Keyspace1", "Standard1", "testing", ConsistencyLevel.ONE, 10, "localhost", 9160));
//		FSDirectory directory = FSDirectory.open(new File("./index"));
		
		IndexReader reader = IndexReader.open(directory);
		int size = reader.numDocs();
		long total = 0;
		int runs = 1000000;
		for (int i = 0; i < runs; i++) {
			if (!reader.isCurrent()) {
				IndexReader indexReader = reader;
				System.out.println("reopening");
				long s = System.currentTimeMillis();
				reader = reader.reopen();
				long e = System.currentTimeMillis();
				System.out.println("reopen took [" + (e-s) + "]");
				size = reader.numDocs();
				indexReader.close();
			}
			IndexSearcher searcher = new IndexSearcher(reader);
			long s = System.currentTimeMillis();
			TopDocs topdocs = searcher.search(new TermQuery(new Term("random1000000",Integer.toString(random.nextInt(1000000)))), 10);
			long e = System.currentTimeMillis();
			total += (e-s);
			System.out.println("search got [" +
					topdocs.totalHits + "] hits in [" + (e-s) +
							"] ms in [" + size +
							"] docs");
			Thread.sleep(100);
		}
		System.out.println("Total [" + total + "] avg [" + (total / (double)runs) + "]");


	}

}
