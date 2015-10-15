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
package org.apache.blur.lucene.security;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import org.apache.blur.lucene.security.index.AccessControlFactory;
import org.apache.blur.lucene.security.index.AccessControlWriter;
import org.apache.blur.lucene.security.index.FilterAccessControlFactory;
import org.apache.blur.lucene.security.search.SecureIndexSearcher;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class LoadTest {

  private static final long MAX_DOCS = 100000000;

  public static void main(String[] args) throws IOException {
    AccessControlFactory accessControlFactory = new FilterAccessControlFactory();
    // AccessControlFactory accessControlFactory = new
    // DocValueAccessControlFactory();
    runTest(accessControlFactory);

  }

  private static void runTest(AccessControlFactory accessControlFactory) throws IOException {
    File file = new File("./src/test/resouces/loadtestindex-" + accessControlFactory.getClass().getName());
    FSDirectory directory = FSDirectory.open(file);
    if (!file.exists() || !DirectoryReader.indexExists(directory)) {
      long s = System.nanoTime();
      createIndex(directory, accessControlFactory);
      long e = System.nanoTime();
      System.out.println("Index Creation Time [" + (e - s) / 1000000.0 + "]");
    }
    DirectoryReader reader = DirectoryReader.open(directory);

    IndexSearcher searcher = new IndexSearcher(reader);

    SecureIndexSearcher secureIndexSearcher1 = new SecureIndexSearcher(reader, accessControlFactory,
        Arrays.asList("nothing"), Arrays.asList("nothing"), new HashSet<String>(), null);

    SecureIndexSearcher secureIndexSearcher2 = new SecureIndexSearcher(reader, accessControlFactory,
        Arrays.asList("r1"), Arrays.asList("nothing"), new HashSet<String>(), null);

    MatchAllDocsQuery query = new MatchAllDocsQuery();
    for (int p = 0; p < 10; p++) {
      hitEnterToContinue();
      runSearch(searcher, query);
      hitEnterToContinue();
      runSearch(secureIndexSearcher1, query);
      hitEnterToContinue();
      runSearch(secureIndexSearcher2, query);
    }
  }

  private static void hitEnterToContinue() throws IOException {
    // System.out.println("Hit Enter.");
    // BufferedReader reader = new BufferedReader(new
    // InputStreamReader(System.in));
    // reader.readLine();
  }

  private static void runSearch(IndexSearcher searcher, MatchAllDocsQuery query) throws IOException {
    long t1 = System.nanoTime();
    TopDocs topDocs = searcher.search(query, 10);
    long t2 = System.nanoTime();
    System.out.println(topDocs.totalHits + " " + (t2 - t1) / 1000000.0);
  }

  private static void createIndex(Directory directory, AccessControlFactory accessControlFactory) throws IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory, conf);

    AccessControlWriter accessControlWriter = accessControlFactory.getWriter();
    Random random = new Random(1);
    for (long i = 0; i < MAX_DOCS; i++) {
      if (i % 1000000 == 0) {
        System.out.println("Building " + i);
      }
      writer.addDocument(accessControlWriter.addDiscoverVisiblity("d1",
          accessControlWriter.addReadVisiblity("r1", getDoc(i, random))));
    }
    writer.close();
  }

  private static Iterable<IndexableField> getDoc(long i, Random random) {
    Document document = new Document();
    document.add(new StringField("f1", Integer.toString(random.nextInt(1000000)), Store.YES));
    return document;
  }

}
