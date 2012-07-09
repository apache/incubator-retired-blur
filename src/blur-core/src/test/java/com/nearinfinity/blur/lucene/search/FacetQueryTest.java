package com.nearinfinity.blur.lucene.search;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FacetQueryTest {

  private IndexReader reader;

  @Before
  public void setup() throws CorruptIndexException, LockObtainFailedException, IOException {
    reader = createIndex();
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testFacetQueryNoSuper() throws IOException {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("f1", "value")), Occur.SHOULD);
    bq.add(new TermQuery(new Term("f2", "v3")), Occur.SHOULD);
    
    Query f1 = new TermQuery(new Term("f2", "v4"));
    
    
    BooleanQuery f2 = new BooleanQuery();
    f2.add(new TermQuery(new Term("f1", "value")), Occur.MUST);
    f2.add(new TermQuery(new Term("f2", "v3")), Occur.MUST);
    
    Query[] facets = new Query[] { f1, f2 };
    
    AtomicLongArray counts = new AtomicLongArray(facets.length);
    FacetQuery facetQuery = new FacetQuery(bq, facets, counts);
    
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    TopDocs topDocs = indexSearcher.search(facetQuery, 10);
    
    for (int i = 0; i < counts.length(); i++) {
      System.out.println(counts.get(i));
    }
  }

  private IndexReader createIndex() throws CorruptIndexException, LockObtainFailedException, IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory, conf);
    for (int i = 0; i < 10; i++) {
      Document document = new Document();
      document.add(new Field("f1", "value", Store.YES, Index.NOT_ANALYZED_NO_NORMS));
      document.add(new Field("f2", "v" + i, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
      writer.addDocument(document);
    }
    writer.close();
    return IndexReader.open(directory);
  }

}
