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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.concurrent.Executors;
import org.apache.blur.trace.LogTraceStorage;
import org.apache.blur.trace.Trace;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

public class FacetQueryTest {

  private static final boolean TRACE = false;

  private int _docCount = 10000;

  @Test
  public void testFacetQueryNoSuper() throws IOException, InterruptedException {
    System.out.println("testFacetQueryNoSuper");
    IndexReader reader = createIndex(10, 0, true);
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("f1", "value")), Occur.SHOULD);
    bq.add(new TermQuery(new Term("f2", "v3")), Occur.SHOULD);

    Query f1 = new TermQuery(new Term("f2", "v4"));

    BooleanQuery f2 = new BooleanQuery();
    f2.add(new TermQuery(new Term("f1", "value")), Occur.MUST);
    f2.add(new TermQuery(new Term("f2", "v3")), Occur.MUST);

    Query[] facets = new Query[] { f1, f2 };

    FacetExecutor facetExecutor = new FacetExecutor(facets.length);
    FacetQuery facetQuery = new FacetQuery(bq, facets, facetExecutor);

    IndexSearcher indexSearcher = new IndexSearcher(reader);
    indexSearcher.search(facetQuery, 10);

    ExecutorService executor = getThreadPool(10);
    facetExecutor.processFacets(executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    for (int i = 0; i < facetExecutor.length(); i++) {
      assertEquals(1L, facetExecutor.get(i));
    }
  }

  @Test
  public void testFacetQueryPerformance1() throws IOException, InterruptedException {
    System.out.println("testFacetQueryPerformance1");
    BlurConfiguration configuration = new BlurConfiguration();
    Trace.setStorage(new LogTraceStorage(configuration));
    int facetCount = 200;

    IndexReader reader = createIndex(_docCount, facetCount, false);

    Query[] facets = new Query[facetCount];
    for (int i = 0; i < facetCount; i++) {
      facets[i] = new TermQuery(new Term("facet" + i, "value"));
    }

    ExecutorService executor = null;
    try {
      for (int t = 0; t < 5; t++) {
        executor = getThreadPool(20);
        IndexSearcher indexSearcher = new IndexSearcher(reader, executor);
        FacetExecutor facetExecutor = new FacetExecutor(facets.length);
        FacetQuery facetQuery = new FacetQuery(new TermQuery(new Term("f1", "value")), facets, facetExecutor);
        long t1 = System.nanoTime();
        indexSearcher.search(facetQuery, 10);
        if (t == 4 && TRACE) {
          Trace.setupTrace("unittest");
        }
        facetExecutor.processFacets(executor);
        if (t == 4 && TRACE) {
          Trace.tearDownTrace();
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        long t2 = System.nanoTime();
        System.out.println((t2 - t1) / 1000000.0);

        for (int i = 0; i < facetExecutor.length(); i++) {
          assertEquals((long) _docCount, facetExecutor.get(i));
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testFacetQueryPerformance2() throws IOException, InterruptedException {
    System.out.println("testFacetQueryPerformance2");
    BlurConfiguration configuration = new BlurConfiguration();
    Trace.setStorage(new LogTraceStorage(configuration));
    int facetCount = 200;
    IndexReader reader = createIndex(_docCount, facetCount, false);

    Query[] facets = new Query[facetCount];
    for (int i = 0; i < facetCount; i++) {
      facets[i] = new TermQuery(new Term("facet" + i, "value"));
    }

    ExecutorService executor = null;
    try {
      for (int t = 0; t < 5; t++) {
        executor = getThreadPool(20);
        IndexSearcher indexSearcher = new IndexSearcher(reader, executor);
        FacetExecutor facetExecutor = new FacetExecutor(facets.length);
        FacetQuery facetQuery = new FacetQuery(new TermQuery(new Term("f2", "v45")), facets, facetExecutor);
        long t1 = System.nanoTime();
        indexSearcher.search(facetQuery, 10);
        if (t == 4 && TRACE) {
          Trace.setupTrace("unittest");
        }
        facetExecutor.processFacets(executor);
        if (t == 4 && TRACE) {
          Trace.tearDownTrace();
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        long t2 = System.nanoTime();
        System.out.println((t2 - t1) / 1000000.0);

        for (int i = 0; i < facetExecutor.length(); i++) {
          assertEquals(1, facetExecutor.get(i));
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testFacetQueryPerformanceWithMins() throws IOException, InterruptedException {
    System.out.println("testFacetQueryPerformanceWithMins");
    int facetCount = 200;
    IndexReader reader = createIndex(_docCount, facetCount, false);

    Query[] facets = new Query[facetCount];
    for (int i = 0; i < facetCount; i++) {
      facets[i] = new TermQuery(new Term("facet" + i, "value"));
    }

    long min = 1000;
    long[] minimumsBeforeReturning = new long[facets.length];
    for (int i = 0; i < minimumsBeforeReturning.length; i++) {
      minimumsBeforeReturning[i] = min;
    }

    ExecutorService executor = null;
    try {
      for (int t = 0; t < 5; t++) {
        executor = getThreadPool(10);
        IndexSearcher indexSearcher = new IndexSearcher(reader, executor);
        FacetExecutor facetExecutor = new FacetExecutor(facets.length, minimumsBeforeReturning);
        FacetQuery facetQuery = new FacetQuery(new TermQuery(new Term("f1", "value")), facets, facetExecutor);
        long t1 = System.nanoTime();
        indexSearcher.search(facetQuery, 10);
        facetExecutor.processFacets(executor);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        long t2 = System.nanoTime();
        System.out.println((t2 - t1) / 1000000.0);

        for (int i = 0; i < facetExecutor.length(); i++) {

          assertTrue(facetExecutor.get(i) >= min);
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private ExecutorService getThreadPool(int threads) {
    return Executors.newThreadPool("unittest-facets", threads);
  }

  private IndexReader createIndex(int docCount, int facetFields, boolean ram) throws CorruptIndexException,
      LockObtainFailedException, IOException {
    Directory directory;
    if (ram) {
      directory = new RAMDirectory();
    } else {
      File dir = new File("./target/tmp/facet_tmp");
      if (dir.exists()) {
        directory = FSDirectory.open(dir);
        if (DirectoryReader.indexExists(directory)) {
          DirectoryReader reader = DirectoryReader.open(directory);
          if (reader.numDocs() == docCount) {
            return reader;
          }
          reader.close();
          directory.close();
        }
      }
      rmr(dir);
      directory = FSDirectory.open(dir);
    }
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory, conf);
    FieldType fieldType = new FieldType();
    fieldType.setStored(true);
    fieldType.setIndexed(true);
    fieldType.setOmitNorms(true);
    long start = System.nanoTime();
    for (int i = 0; i < docCount; i++) {
      long now = System.nanoTime();
      if (start + TimeUnit.SECONDS.toNanos(5) < now) {
        System.out.println("Indexing doc " + i + " of " + docCount);
        start = System.nanoTime();
      }
      Document document = new Document();
      document.add(new Field("f1", "value", fieldType));
      document.add(new Field("f2", "v" + i, fieldType));
      for (int f = 0; f < facetFields; f++) {
        document.add(new Field("facet" + f, "value", fieldType));
      }
      writer.addDocument(document);
    }
    writer.close();
    return DirectoryReader.open(directory);
  }

  private void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }

}
