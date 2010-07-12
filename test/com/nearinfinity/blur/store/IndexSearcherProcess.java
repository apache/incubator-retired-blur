package com.nearinfinity.blur.store;

import java.io.File;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.store.policy.ZookeeperIndexDeletionPolicy;

public class IndexSearcherProcess {

	public static void main(String[] args) throws Exception {
		final ZooKeeper zk = new ZooKeeper("localhost", 3000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
		
		final String indexRefPath = "/blur/refs/testing";
		FSDirectory dir = FSDirectory.open(new File("./index"));
		final ZookeeperWrapperDirectory directory = new ZookeeperWrapperDirectory(zk, dir, indexRefPath);
		
		while (!IndexReader.indexExists(directory)) {
			Thread.sleep(100);
		}

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

}
