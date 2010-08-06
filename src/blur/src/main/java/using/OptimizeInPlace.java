package using;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

public class OptimizeInPlace {

	public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException {
		IndexWriter writer = new IndexWriter(FSDirectory.open(new File("/Users/amccurry/testIndex")), new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		writer.optimize();
		writer.close();
	}

}
