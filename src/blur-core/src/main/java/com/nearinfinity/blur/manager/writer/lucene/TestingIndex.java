package com.nearinfinity.blur.manager.writer.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class TestingIndex {

  public static void main(String[] args) throws IOException {
    File file = new File("./index-rt");
    rm(file);
    Directory directory = FSDirectory.open(file);
    RTIndex index = new RTIndex(directory, new StandardAnalyzer(Version.LUCENE_35), 1000);
    startSearchers(index, 1);
    long s = System.currentTimeMillis();
    long total = 0;
    long last = s;
    for (int i = 0; i < 1000000; i++) {
      long now = System.currentTimeMillis();
      if (last + 5000 < now) {
        double seconds = (now - s) / 1000.0;
        double rate = i / seconds;
        System.out.println(rate);
        last = now;
      }
      IndexReader reader = index.getIndexReader();
      int numDocs = reader.numDocs();
      total += numDocs;
      reader.close();
      index.addDocuments(Arrays.asList(getDocument()));
    }
    System.out.println(System.currentTimeMillis() - s + " " + total);

  }

  private static void startSearchers(RTIndex index, int size) {
    for (int i = 0; i < size; i++) {
      startSearcher(index);
    }
  }

  private static void startSearcher(final RTIndex index) {
    new Thread(new Runnable() {
      @Override
      public void run() {

        while (true) {
          try {
            long start = System.nanoTime();
            IndexReader reader = index.getIndexReader();
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(new TermQuery(new Term("name", "value")), 10);
            reader.close();
            long end = System.nanoTime();
            System.out.println(topDocs.totalHits + " " + (end - start) / 1000000.0);
          } catch (IOException e) {
            e.printStackTrace();
          }
          try {
            Thread.sleep(250);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    }).start();
  }

  private static void rm(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }

  public static Document getDocument() {
    Document document = new Document();
    for (int i = 0; i < 10; i++) {
      document.add(new Field("id" + i, UUID.randomUUID().toString(), Store.YES, Index.ANALYZED_NO_NORMS));
    }
    document.add(new Field("name", "value", Store.YES, Index.ANALYZED_NO_NORMS));
    return document;
  }

}
