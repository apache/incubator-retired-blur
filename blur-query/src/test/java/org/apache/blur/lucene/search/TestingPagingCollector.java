package org.apache.blur.lucene.search;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.lucene.search.IterablePaging.ProgressRef;
import org.apache.blur.lucene.search.IterablePaging.TotalHitsRef;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.utils.BlurIterator;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntDocValuesField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

/**
 * Testing the paging collector.
 * 
 */
public class TestingPagingCollector {

  private static RAMDirectory _directory;
  private int length = 1324;

  @Test
  public void testSimpleSearchPaging() throws Exception {
    IndexReader reader = getReaderFlatScore(length);
    IndexSearcherCloseable searcher = getSearcher(reader);

    TotalHitsRef totalHitsRef = new TotalHitsRef();
    ProgressRef progressRef = new ProgressRef();

    TermQuery query = new TermQuery(new Term("f1", "value"));
    IterablePaging paging = new IterablePaging(new AtomicBoolean(true), searcher, query, 100, null, null, false, null,
        new DeepPagingCache());
    IterablePaging itPaging = paging.skipTo(90).gather(20).totalHits(totalHitsRef).progress(progressRef);
    BlurIterator<ScoreDoc, BlurException> iterator = itPaging.iterator();
    int position = 90;
    int searches = 1;
    while (iterator.hasNext()) {
      ScoreDoc sd = iterator.next();
      assertEquals(position, progressRef.currentHitPosition());
      assertEquals(searches, progressRef.searchesPerformed());
      System.out.println("time [" + progressRef.queryTime() + "] " + "total hits [" + totalHitsRef.totalHits() + "] "
          + "searches [" + progressRef.searchesPerformed() + "] " + "position [" + progressRef.currentHitPosition()
          + "] " + "doc id [" + sd.doc + "] " + "score [" + sd.score + "]");
      position++;
      if (position == 100) {
        searches++;
      }
    }
  }

  private IndexSearcherCloseable getSearcher(IndexReader reader) {
    return new IndexSearcherCloseableBase(reader, null) {
      @Override
      public Directory getDirectory() {
        return _directory;
      }

      @Override
      public void close() throws IOException {
        // Do nothing because it's in memory test.
      }
    };
  }

  @Test
  public void testSimpleSearchPagingThroughAll() throws Exception {
    IndexReader reader = getReaderFlatScore(length);
    IndexSearcherCloseable searcher = getSearcher(reader);

    TotalHitsRef totalHitsRef = new TotalHitsRef();
    ProgressRef progressRef = new ProgressRef();

    long start = System.nanoTime();
    int position = 0;
    DeepPagingCache deepPagingCache = new DeepPagingCache(10);
    OUTER: while (true) {
      MatchAllDocsQuery query = new MatchAllDocsQuery();
      IterablePaging paging = new IterablePaging(new AtomicBoolean(true), searcher, query, 100, null, null, false,
          null, deepPagingCache);
      IterablePaging itPaging = paging.skipTo(position).totalHits(totalHitsRef).progress(progressRef);
      BlurIterator<ScoreDoc, BlurException> iterator = itPaging.iterator();

      while (iterator.hasNext()) {
        ScoreDoc sd = iterator.next();
        assertEquals(position, progressRef.currentHitPosition());
        System.out.println("time [" + progressRef.queryTime() + "] " + "total hits [" + totalHitsRef.totalHits() + "] "
            + "searches [" + progressRef.searchesPerformed() + "] " + "position [" + progressRef.currentHitPosition()
            + "] " + "doc id [" + sd.doc + "] " + "score [" + sd.score + "]");
        position++;
        if (position % 100 == 0) {
          continue OUTER;
        }
      }
      break OUTER;
    }
    long end = System.nanoTime();
    assertEquals(length, position);
    System.out.println("Took [" + (end - start) / 1000000.0 + " ms]");
  }

  @Test
  public void testSimpleSearchPagingWithSorting() throws Exception {
    IndexReader reader = getReaderFlatScore(length);
    IndexSearcherCloseable searcher = getSearcher(reader);

    TotalHitsRef totalHitsRef = new TotalHitsRef();
    ProgressRef progressRef = new ProgressRef();

    printHeapSize();
    TermQuery query = new TermQuery(new Term("f1", "value"));
    Sort sort = new Sort(new SortField("index", Type.INT, true));
    IterablePaging paging = new IterablePaging(new AtomicBoolean(true), searcher, query, 100, null, null, false, sort,
        new DeepPagingCache());
    IterablePaging itPaging = paging.skipTo(90).gather(20).totalHits(totalHitsRef).progress(progressRef);
    BlurIterator<ScoreDoc, BlurException> iterator = itPaging.iterator();
    int position = 90;
    int searches = 1;
    while (iterator.hasNext()) {
      ScoreDoc sd = iterator.next();
      assertEquals(position, progressRef.currentHitPosition());
      assertEquals(searches, progressRef.searchesPerformed());
      System.out.println("time [" + progressRef.queryTime() + "] " + "total hits [" + totalHitsRef.totalHits() + "] "
          + "searches [" + progressRef.searchesPerformed() + "] " + "position [" + progressRef.currentHitPosition()
          + "] " + "doc id [" + sd.doc + "] " + "score [" + sd.score + "]");
      position++;
      if (position == 100) {
        searches++;
      }
    }
    printHeapSize();
  }

  private void printHeapSize() {
    System.gc();
    System.gc();
    System.out.println("heap size=" + ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
  }

  private static IndexReader getReaderFlatScore(int length) throws Exception {
    _directory = new RAMDirectory();
    IndexWriter indexWriter = new IndexWriter(_directory, new IndexWriterConfig(LUCENE_VERSION, new KeywordAnalyzer()));
    for (int i = 0; i < length; i++) {
      Document document = new Document();
      document.add(new StringField("f1", "value", Store.NO));
      document.add(new IntDocValuesField("index", i));
      document.add(new IntField("index", i, Store.YES));
      indexWriter.addDocument(document);
    }
    indexWriter.close();
    return DirectoryReader.open(_directory);
  }

  static byte[] toBytes(int val) {
    byte[] b = new byte[4];
    b[3] = (byte) (val);
    b[2] = (byte) (val >>> 8);
    b[1] = (byte) (val >>> 16);
    b[0] = (byte) (val >>> 24);
    return b;
  }

  static int toInt(byte[] b) {
    return ((b[3] & 0xFF)) + ((b[2] & 0xFF) << 8) + ((b[1] & 0xFF) << 16) + ((b[0]) << 24);
  }

}
