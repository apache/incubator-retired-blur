package using;

import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.index.SuperDocument;
import com.nearinfinity.blur.index.SuperIndexWriter;
import com.nearinfinity.blur.search.SuperQuery;
import com.nearinfinity.blur.search.SuperSearcher;

public class Main {

	public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException {
		SuperDocument document = new SuperDocument("1234");
		document.addFieldAnalyzedNoNorms("test", "test", "cf1", "5678");
		
		Directory directory = new RAMDirectory();
		
		SuperIndexWriter indexWriter = new SuperIndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		indexWriter.addSuperDocument(document);
		indexWriter.close();
		
		SuperSearcher searcher = new SuperSearcher(directory);
		TopDocs topDocs = searcher.search(new SuperQuery(new TermQuery(new Term("test","test"))), 10);
		System.out.println(topDocs.totalHits);
		System.out.println(searcher.superDoc(topDocs.scoreDocs[0].doc));
	}

}
