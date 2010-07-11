package com.nearinfinity.blur.search;

import java.io.IOException;
import java.util.UUID;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.index.SuperDocument;
import com.nearinfinity.blur.index.SuperIndexReader;
import com.nearinfinity.blur.index.SuperIndexWriter;

public class SuperQueryTest extends TestCase {
	
	public void testSimpleSuperQuery() throws CorruptIndexException, IOException, InterruptedException {
		BooleanQuery booleanQuery = new BooleanQuery();
		booleanQuery.add(wrapSuper(new TermQuery(new Term("person.name","aaron"))), Occur.MUST);
		booleanQuery.add(wrapSuper(new TermQuery(new Term("address.street","sulgrave"))), Occur.MUST);
		
		Directory directory = createIndex();
		SuperIndexReader reader = new SuperIndexReader(IndexReader.open(directory));
		reader.waitForWarmUp();
		IndexSearcher searcher = new IndexSearcher(reader);
		TopDocs topDocs = searcher.search(booleanQuery, 10);
		assertEquals(2, topDocs.totalHits);
		assertEquals("1",searcher.doc(topDocs.scoreDocs[0].doc).get(SuperDocument.ID));
		assertEquals("3",searcher.doc(topDocs.scoreDocs[1].doc).get(SuperDocument.ID));
		
	}

	private Directory createIndex() throws CorruptIndexException, LockObtainFailedException, IOException {
		Directory directory = new RAMDirectory();
		SuperIndexWriter writer = new SuperIndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		writer.addSuperDocument(create("1","person.name:aaron","address.street:sulgrave"));
		writer.addSuperDocument(create("2","person.name:hannah","address.street:sulgrave"));
		writer.addSuperDocument(create("3","person.name:aaron","address.street:sulgrave court"));
		writer.close();
		return directory;
	}

	private SuperDocument create(String id, String... parts) {
		SuperDocument document = new SuperDocument(id);
		for (String part : parts) {
			String[] split = part.split(":");
			String value = split[1];
			String[] split2 = split[0].split("\\.");
			String superName = split2[0];
			String fieldName = split2[1];
			document.addFieldNotAnalyzedNoNorms(superName, UUID.randomUUID().toString(), fieldName, value);
		}
		return document;
	}

	private Query wrapSuper(Query query) {
		return new SuperQuery(query);
	}

}
