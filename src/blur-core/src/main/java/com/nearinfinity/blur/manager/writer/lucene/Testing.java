package com.nearinfinity.blur.manager.writer.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

public class Testing {

  public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException {
    FSDirectory directory = FSDirectory.open(new File("./index"));
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory, conf);
    for (int i = 0; i < 1000000; i++) {
      writer.addDocument(getDoc(i));
    }
    writer.close(true);

    IndexReader reader = IndexReader.open(directory);
    IndexReader indexReader = SoftDeleteIndexReader.wrap(reader, Arrays.asList(new Term("id","2")));
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TopDocs topDocs = searcher.search(new TermQuery(new Term("name","value")), 10);
    System.out.println(topDocs.totalHits);
  }

  private static Document getDoc(int i) {
    Document document = new Document();
    document.add(new Field("id", Integer.toString(i), Store.YES, Index.ANALYZED_NO_NORMS));
    document.add(new Field("name", "value", Store.YES, Index.ANALYZED_NO_NORMS));
    return document;
  }

}
