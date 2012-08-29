package com.nearinfinity.blur.analysis;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

import com.nearinfinity.blur.index.IndexWriter;

public class LongAnalyzerTest {

  @Test
  public void testLongAnalyzer() throws IOException {
    LongAnalyzer analyzer = new LongAnalyzer("long");
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_36, analyzer);
    Directory dir = new RAMDirectory();
    IndexWriter indexWriter = new IndexWriter(dir, conf);
    Random random = new Random();
    int max = 17;
    for (int i = 0; i < 100; i++) {
      int v = random.nextInt(max);
      Document document = new Document();
      document.add(new Field("f1", Long.toString(v), Store.YES, Index.ANALYZED_NO_NORMS));
      FieldConverterUtil.convert(document, analyzer);
      indexWriter.addDocument(document);
    }
    indexWriter.close();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    NumericRangeQuery<Long> query = NumericRangeQuery.newLongRange("f1", 0L, 2L, true, true);
    Query rewrite = searcher.rewrite(query);
    TopDocs docs = searcher.search(rewrite, 100);
    ScoreDoc[] scoreDocs = docs.scoreDocs;
    for (int i = 0; i < docs.totalHits; i++) {
      Document document = searcher.doc(scoreDocs[i].doc);
      assertTrue(Integer.parseInt(document.get("f1")) < 3);
    }
  }

}
