package using;

import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.lucene.index.SuperIndexReader;
import com.nearinfinity.blur.lucene.search.SuperParser;

public class CreateTestIndex {

	public static void main(String[] args) throws IOException, ParseException, InterruptedException {
		
		SuperDocument doc1 = new SuperDocument("1");
		doc1.addFieldAnalyzedNoNorms("person", "1", "name", "Aaron McCurry");
		doc1.addFieldAnalyzedNoNorms("person", "2", "name", "Aaron Patrick McCurry");
		doc1.addFieldAnalyzedNoNorms("address", "1", "street", "133 Sulgrave Court");
		
		SuperDocument doc2 = new SuperDocument("2");
		doc2.addFieldAnalyzedNoNorms("person", "1", "name", "Hannah McCurry");
		doc2.addFieldAnalyzedNoNorms("person", "2", "name", "Hannah Mele McCurry");
		doc2.addFieldAnalyzedNoNorms("person", "3", "name", "Hannah Mele Hite");
		doc2.addFieldAnalyzedNoNorms("person", "4", "name", "Hannah Hite");
		doc2.addFieldAnalyzedNoNorms("address", "1", "street", "133 Sulgrave Court");
		
		SuperDocument doc3 = new SuperDocument("3");
		doc3.addFieldAnalyzedNoNorms("person", "1", "name", "Eva McCurry");
		doc3.addFieldAnalyzedNoNorms("person", "2", "name", "Eva Mele McCurry");
		doc3.addFieldAnalyzedNoNorms("address", "1", "street", "133 Sulgrave Court");
		
		Directory directory = new RAMDirectory();
		IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		add(writer,doc1);
		add(writer,doc2);
		add(writer,doc3);
		writer.close();
		
		SuperIndexReader reader = new SuperIndexReader(directory);
		reader.waitForWarmUp();
		IndexSearcher searcher = new IndexSearcher(reader);
		
		Filter queryFilter = new QueryWrapperFilter(new MatchAllDocsQuery());
		Query q = new SuperParser(Version.LUCENE_CURRENT, new StandardAnalyzer(Version.LUCENE_CURRENT), 
				true, queryFilter).parse("person.name:mccurry");
		
		TopDocs topDocs = searcher.search(q, 10);
		System.out.println(topDocs.totalHits);
		
		for (int i = 0; i < topDocs.scoreDocs.length; i++) {
			System.out.println(searcher.doc(topDocs.scoreDocs[i].doc) + " " + topDocs.scoreDocs[i].score);
		}
	}

	private static void add(IndexWriter writer, SuperDocument superDocument) throws IOException {
		for (Document document : superDocument.getAllDocumentsForIndexing()) {
			System.out.println("Adding [" + document + "]");
			writer.addDocument(document);
		}
	}

}
