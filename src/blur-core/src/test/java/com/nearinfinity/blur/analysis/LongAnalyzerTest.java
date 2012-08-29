package com.nearinfinity.blur.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

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
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.ColumnDefinition;
import com.nearinfinity.blur.thrift.generated.ColumnFamilyDefinition;

public class LongAnalyzerTest {

  @Test
  public void testLongAnalyzer() throws IOException {
    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    ColumnFamilyDefinition cfDef = new ColumnFamilyDefinition();
    cfDef.putToColumnDefinitions("test", new ColumnDefinition("long", true, null));
    analyzerDefinition.putToColumnFamilyDefinitions("test", cfDef);
    BlurAnalyzer analyzer = new BlurAnalyzer(analyzerDefinition);

    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_36, analyzer);
    Directory dir = new RAMDirectory();
    IndexWriter indexWriter = new IndexWriter(dir, conf);
    for (int i = 0; i < 1000; i++) {
      Document document = new Document();
      String value = Long.toString(i);
      document.add(new Field("test.test", value, Store.YES, Index.ANALYZED_NO_NORMS));
      FieldConverterUtil.convert(document, analyzer);
      indexWriter.addDocument(document);
    }
    indexWriter.close();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    NumericRangeQuery<Long> query = NumericRangeQuery.newLongRange("test.test", 0L, 2L, true, true);
    Query rewrite = searcher.rewrite(query);
    TopDocs docs = searcher.search(rewrite, 100);
    ScoreDoc[] scoreDocs = docs.scoreDocs;
    assertEquals(3, docs.totalHits);
    for (int i = 0; i < docs.totalHits; i++) {
      Document document = searcher.doc(scoreDocs[i].doc);
      assertTrue(Integer.parseInt(document.get("test.test")) < 3);
    }
  }
}
