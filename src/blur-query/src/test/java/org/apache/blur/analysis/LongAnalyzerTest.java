package org.apache.blur.analysis;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.blur.index.IndexWriter;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.ColumnFamilyDefinition;
import org.apache.lucene.analysis.Analyzer;
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

public class LongAnalyzerTest {

  @Test
  public void testLongAnalyzer() throws IOException {
    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    Map<String, ColumnDefinition> columnDefinitions = new HashMap<String, ColumnDefinition>();
    columnDefinitions.put("test", new ColumnDefinition("long", false, null));
    ColumnFamilyDefinition val = new ColumnFamilyDefinition(null, columnDefinitions);
    analyzerDefinition.putToColumnFamilyDefinitions("test", val);
    Analyzer analyzer = new BlurAnalyzer(analyzerDefinition);
    runTestString(analyzer, 10);
  }

  @Test
  public void testLongAnalyzerDifferentStep() throws IOException {

    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    Map<String, ColumnDefinition> columnDefinitions = new HashMap<String, ColumnDefinition>();
    columnDefinitions.put("test", new ColumnDefinition("long,4", false, null));
    ColumnFamilyDefinition val = new ColumnFamilyDefinition(null, columnDefinitions);
    analyzerDefinition.putToColumnFamilyDefinitions("test", val);
    Analyzer analyzer = new BlurAnalyzer(analyzerDefinition);
    runTestString(analyzer, 10);
  }

  @Test
  public void testLongAnalyzerDifferentStepAndRadix() throws IOException {
    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    Map<String, ColumnDefinition> columnDefinitions = new HashMap<String, ColumnDefinition>();
    columnDefinitions.put("test", new ColumnDefinition("long,4,16", false, null));
    ColumnFamilyDefinition val = new ColumnFamilyDefinition(null, columnDefinitions);
    analyzerDefinition.putToColumnFamilyDefinitions("test", val);
    Analyzer analyzer = new BlurAnalyzer(analyzerDefinition);
    runTestString(analyzer, 16);
  }

  private void runTestString(Analyzer analyzer, int radix) throws IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_36, analyzer);
    Directory dir = new RAMDirectory();
    IndexWriter indexWriter = new IndexWriter(dir, conf);
    for (int i = 0; i < 1000; i++) {
      Document document = new Document();
      String value = Long.toString(i, radix);
      document.add(new Field("test.test", value, Store.YES, Index.ANALYZED_NO_NORMS));
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
      assertTrue(Long.parseLong(document.get("test.test"), radix) < 3);
    }
  }
}
