package org.apache.blur.store;

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
import static org.apache.blur.lucene.LuceneConstant.LUCENE_VERSION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.NoLockFactory;


public class UsingHdfsDir {

  public static void main(String[] args) throws IOException {

    // FileSystem fs = FileSystem.getLocal(new Configuration());
    // Path p = new Path("file:///tmp/testdir");

    Path p = new Path("hdfs://localhost:9000/test-dir");
    FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
    fs.delete(p, true);

    final HdfsDirectory directory = new HdfsDirectory(p);
    directory.setLockFactory(NoLockFactory.getNoLockFactory());

    IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(LUCENE_VERSION, new StandardAnalyzer(LUCENE_VERSION)));
    for (int i = 0; i < 100000; i++) {
      writer.addDocument(getDoc());
    }
    writer.close();

    IndexReader reader = IndexReader.open(directory);
    TermEnum terms = reader.terms();
    while (terms.next()) {
      System.out.println(terms.term());
    }
    terms.close();

    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs topDocs = searcher.search(new TermQuery(new Term("name", "ffff")), 10);
    System.out.println(topDocs.totalHits);

    reader.close();

    List<String> files = new ArrayList<String>(Arrays.asList(directory.listAll()));
    Collections.sort(files, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        try {
          long fileLength1 = directory.fileLength(o1);
          long fileLength2 = directory.fileLength(o2);
          if (fileLength1 == fileLength2) {
            return o1.compareTo(o2);
          }
          return (int) (fileLength2 - fileLength1);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    for (String file : files) {
      System.out.println(file + " " + directory.fileLength(file));
    }

    directory.close();
  }

  private static Document getDoc() {
    Document document = new Document();
    document.add(new Field("name", UUID.randomUUID().toString(), Store.YES, Index.ANALYZED_NO_NORMS));
    return document;
  }

}
