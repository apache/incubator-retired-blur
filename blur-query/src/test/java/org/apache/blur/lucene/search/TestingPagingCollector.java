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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.lucene.search.IterablePaging.ProgressRef;
import org.apache.blur.lucene.search.IterablePaging.TotalHitsRef;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.utils.BlurIterator;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

/**
 * Testing the paging collector.
 * 
 */
public class TestingPagingCollector {

  @Test
  public void testNothingYet() {

  }

  public static void main(String[] args) throws Exception {
    IndexReader reader = getReaderFlatScore(13245);
    IndexSearcher searcher = new IndexSearcher(reader);

    TotalHitsRef totalHitsRef = new TotalHitsRef();
    ProgressRef progressRef = new ProgressRef();

    TermQuery query = new TermQuery(new Term("f1", "value"));
    IterablePaging paging = new IterablePaging(new AtomicBoolean(true), searcher, query, 100, null, null, false);
    IterablePaging itPaging = paging.skipTo(90).gather(20).totalHits(totalHitsRef).progress(progressRef);
    BlurIterator<ScoreDoc, BlurException> iterator = itPaging.iterator();
    while (iterator.hasNext()) {
      ScoreDoc sd = iterator.next(); 
      System.out.println("time [" + progressRef.queryTime() + "] " + "total hits [" + totalHitsRef.totalHits() + "] "
          + "searches [" + progressRef.searchesPerformed() + "] " + "position [" + progressRef.currentHitPosition()
          + "] " + "doc id [" + sd.doc + "] " + "score [" + sd.score + "]");
    }
  }

  private static IndexReader getReaderFlatScore(int length) throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(LUCENE_VERSION, new KeywordAnalyzer()));
    for (int i = 0; i < length; i++) {
      Document document = new Document();
      document.add(new Field("f1", "value", Store.NO, Index.ANALYZED_NO_NORMS));
      indexWriter.addDocument(document);
    }
    indexWriter.close();
    return IndexReader.open(directory);
  }

}
