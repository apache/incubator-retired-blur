package using;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.search.cache.facet.Facet;
import com.nearinfinity.blur.search.cache.facet.FacetManager;

public class UsingFacets {

	public static void main(String[] args) throws CorruptIndexException, IOException {
		
		FacetManager facetManager = new FacetManager("test", false);
		facetManager.add("t1", new TermQuery(new Term("t","t1")));
		facetManager.add("t2", new TermQuery(new Term("t","t2")));
		
		Directory directory = genIndex();
		IndexReader reader = IndexReader.open(directory);
		
		facetManager.update(reader);
		facetManager.updateAllFacets();
		
		IndexSearcher searcher = new IndexSearcher(reader);
		Facet facet = facetManager.create("t1","t2");
		searcher.search(new TermQuery(new Term("name","value")), facet);
		System.out.println(facet.getCounts());
		
		
	}
	
	private static Directory genIndex() throws CorruptIndexException, IOException {
		RAMDirectory directory = new RAMDirectory();
		IndexWriter indexWriter = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		
		indexWriter.addDocument(getDoc());
		indexWriter.addDocument(getDoc());
		indexWriter.addDocument(getDoc());
		
		indexWriter.close();
		
		return directory;
	}

	private static Document getDoc() {
		Document document = new Document();
		document.add(new Field("name","value",Store.YES,Index.ANALYZED_NO_NORMS));
		document.add(new Field("id",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
		document.add(new Field("t",rand() ? "t1" : "t2",Store.YES,Index.ANALYZED_NO_NORMS));
		return document;
	}

	private static Random random = new Random();
	
	private static boolean rand() {
		return random.nextBoolean();
	}

}
