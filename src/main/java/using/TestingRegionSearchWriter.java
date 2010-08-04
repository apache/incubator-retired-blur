package using;

import java.net.URI;
import java.util.UUID;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.lucene.index.SuperIndexWriter;
import com.nearinfinity.blur.lucene.store.URIDirectory;
import com.nearinfinity.blur.lucene.store.policy.ZookeeperIndexDeletionPolicy;
import com.nearinfinity.blur.zookeeper.ZooKeeperFactory;

public class TestingRegionSearchWriter {
	
//	private String[] columnFamilies = new String[]{"person","email","telephones"};

	public static void main(String[] args) throws Exception {
//		String baseDir = "/Users/amccurr"
		URI uri = new URI("zk://localhost/blur/test/testIndex?file:///Users/amccurry/testIndex");
		Directory dir = URIDirectory.openDirectory(uri);
		IndexDeletionPolicy indexDeletionPolicy = new ZookeeperIndexDeletionPolicy(ZooKeeperFactory.getZooKeeper(), uri);
		SuperIndexWriter writer = new SuperIndexWriter(dir, new StandardAnalyzer(Version.LUCENE_CURRENT), indexDeletionPolicy, MaxFieldLength.UNLIMITED);
		for (int j = 0; j < 1000; j++) {
			for (int i = 0; i < 1000; i++) {
				writer.addSuperDocument(genDoc());
			}
			writer.optimize();
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
