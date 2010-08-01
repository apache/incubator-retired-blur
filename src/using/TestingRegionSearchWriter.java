package using;

import java.io.File;
import java.util.UUID;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.lucene.index.SuperIndexWriter;
import com.nearinfinity.blur.lucene.store.ZookeeperWrapperDirectory;
import com.nearinfinity.blur.lucene.store.policy.ZookeeperIndexDeletionPolicy;

public class TestingRegionSearchWriter {

	public static void main(String[] args) throws Exception {
//		IndexReader reader = IndexReader.open(FSDirectory.open(new File("/Users/amccurry/testIndex")));
//		System.out.println(reader.numDocs());
//		IndexSearcher searcher = new IndexSearcher(reader);
//		TopDocs topDocs = searcher.search(new TermQuery(new Term("test.test","value")), 10);
//
//		System.out.println(topDocs.totalHits);
//		
		ZooKeeper zk = new ZooKeeper("localhost", 3000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
		ZookeeperWrapperDirectory dir = new ZookeeperWrapperDirectory(FSDirectory.open(new File("/Users/amccurry/testIndex")),"/blur/Users/amccurry/testIndex");
		IndexDeletionPolicy indexDeletionPolicy = new ZookeeperIndexDeletionPolicy(zk, "/blur/indexRefs/Users/amccurry/testIndex");
		SuperIndexWriter writer = new SuperIndexWriter(dir, new StandardAnalyzer(Version.LUCENE_CURRENT), indexDeletionPolicy, MaxFieldLength.UNLIMITED);
		for (int j = 0; j < 1000; j++) {
			for (int i = 0; i < 1000; i++) {
				writer.addSuperDocument(genDoc());
	//			writer.commit();
	//			System.out.println("Commit");
//				Thread.sleep(1000);
			}
			writer.commit();
		}
		writer.close();

	}

	private static SuperDocument genDoc() {
		SuperDocument document = new SuperDocument(UUID.randomUUID().toString());
		document.addFieldAnalyzedNoNorms("test", "1", "test", "value");
		return document;
	}

}
