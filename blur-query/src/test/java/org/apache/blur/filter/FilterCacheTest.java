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
package org.apache.blur.filter;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.TreeSet;

import org.apache.blur.lucene.codec.Blur021Codec;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class FilterCacheTest {

  @Test
  public void test1() throws IOException {
    Filter filter = new QueryWrapperFilter(new TermQuery(new Term("f1", "t1")));
    FilterCache filterCache = new FilterCache("filter1", filter);
    RAMDirectory directory = new RAMDirectory();
    writeDocs(filterCache, directory);

    DirectoryReader reader = DirectoryReader.open(directory);

    IndexSearcher searcher = new IndexSearcher(reader);

    Query query = new TermQuery(new Term("f2", "t2"));
    TopDocs topDocs1 = searcher.search(query, filterCache, 10);
    assertEquals(1, filterCache.getMisses());
    assertEquals(0, filterCache.getHits());
    assertEquals(1, topDocs1.totalHits);

    TopDocs topDocs2 = searcher.search(query, filterCache, 10);
    assertEquals(1, filterCache.getMisses());
    assertEquals(1, filterCache.getHits());
    assertEquals(1, topDocs2.totalHits);

    TopDocs topDocs3 = searcher.search(query, filterCache, 10);
    assertEquals(1, filterCache.getMisses());
    assertEquals(2, filterCache.getHits());
    assertEquals(1, topDocs3.totalHits);
  }

  @Test
  public void test2() throws IOException {
    Filter filter = new QueryWrapperFilter(new TermQuery(new Term("f1", "t1")));
    FilterCache filterCache = new FilterCache("filter1", filter);
    RAMDirectory directory = new RAMDirectory();
    writeDocs(filterCache, directory);
    DirectoryReader reader = DirectoryReader.open(directory);

    IndexSearcher searcher = new IndexSearcher(reader);

    Query query = new TermQuery(new Term("f2", "t2"));
    TopDocs topDocs1 = searcher.search(query, filterCache, 10);
    assertEquals(1, filterCache.getMisses());
    assertEquals(0, filterCache.getHits());
    assertEquals(1, topDocs1.totalHits);

    TopDocs topDocs2 = searcher.search(query, filterCache, 10);
    assertEquals(1, filterCache.getMisses());
    assertEquals(1, filterCache.getHits());
    assertEquals(1, topDocs2.totalHits);

    TopDocs topDocs3 = searcher.search(query, filterCache, 10);
    assertEquals(1, filterCache.getMisses());
    assertEquals(2, filterCache.getHits());
    assertEquals(1, topDocs3.totalHits);

    System.out.println("===============");
    for (String s : new TreeSet<String>(Arrays.asList(directory.listAll()))) {
      System.out.println(s);
    }

    writeDocs(filterCache, directory);

    System.out.println("===============");
    for (String s : new TreeSet<String>(Arrays.asList(directory.listAll()))) {
      System.out.println(s);
    }
  }

  private void writeDocs(FilterCache filterCache, RAMDirectory directory) throws IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
    conf.setCodec(new Blur021Codec());
    IndexWriter indexWriter = new IndexWriter(directory, conf);
    int count = 10000;
    addDocs(indexWriter, count);
    indexWriter.close();
  }

  private void addDocs(IndexWriter indexWriter, int count) throws IOException {
    for (int i = 0; i < count; i++) {
      Document document = new Document();
      document.add(new StringField("f1", "t" + i, Store.YES));
      document.add(new StringField("f2", "t2", Store.YES));
      indexWriter.addDocument(document);
    }
  }

}
