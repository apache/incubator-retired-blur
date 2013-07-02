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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongArray;

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
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
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
    indexSearcher.search(facetQuery, 10);

    //@TODO add actual assertion
    for (int i = 0; i < counts.length(); i++) {
      System.out.println(counts.get(i));
    }
  }

  private IndexReader createIndex() throws CorruptIndexException, LockObtainFailedException, IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory, conf);
    FieldType fieldType = new FieldType();
    fieldType.setStored(true);
    fieldType.setIndexed(true);
    fieldType.setOmitNorms(true);
    for (int i = 0; i < 10; i++) {
      Document document = new Document();
      
      document.add(new Field("f1", "value", fieldType));
      document.add(new Field("f2", "v" + i, fieldType));
      writer.addDocument(document);
    }
    writer.close();
    return DirectoryReader.open(directory);
  }

}
