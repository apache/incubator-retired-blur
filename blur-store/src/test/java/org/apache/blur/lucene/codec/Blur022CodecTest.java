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
package org.apache.blur.lucene.codec;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class Blur022CodecTest {

  private static final int WORDS = 10000;

  @Test
  public void testLargeDocs() throws IOException {
    Random random = new Random();
    Iterable<? extends IndexableField> doc = getLargeDoc(random);
    RAMDirectory directory = new RAMDirectory();
    IndexWriterConfig conf1 = new IndexWriterConfig(Version.LUCENE_43, new WhitespaceAnalyzer(Version.LUCENE_43));
    conf1.setCodec(new Blur022Codec());
    IndexWriter writer1 = new IndexWriter(directory, conf1);
    writer1.addDocument(doc);
    writer1.close();

    DirectoryReader reader1 = DirectoryReader.open(directory);
    int numDocs1 = reader1.numDocs();
    assertEquals(1, numDocs1);

    // for (int i = 0; i < numDocs1; i++) {
    // System.out.println(reader1.document(i));
    // }

    IndexWriterConfig conf2 = new IndexWriterConfig(Version.LUCENE_43, new WhitespaceAnalyzer(Version.LUCENE_43));
    conf2.setCodec(new Blur022Codec(1 << 16, CompressionMode.HIGH_COMPRESSION));
    IndexWriter writer2 = new IndexWriter(directory, conf2);
    writer2.addDocument(doc);
    writer2.close();

    DirectoryReader reader2 = DirectoryReader.open(directory);
    int numDocs2 = reader2.numDocs();
    assertEquals(2, numDocs2);

    for (int i = 0; i < 2; i++) {

      long t1 = System.nanoTime();
      Document document1 = reader1.document(0);
      long t2 = System.nanoTime();
      Document document2 = reader2.document(1);
      long t3 = System.nanoTime();

      System.out.println((t3 - t2) / 1000000.0);
      System.out.println((t2 - t1) / 1000000.0);

      System.out.println("doc1 " + document1.hashCode());
      System.out.println("doc2 " + document2.hashCode());
    }

    // for (int i = 0; i < numDocs2; i++) {
    // System.out.println(reader2.document(i));
    // }

    // long fileLength = directory.fileLength("_0.fdt");

    for (String name : directory.listAll()) {
      if (name.endsWith(".fdt")) {
        System.out.println(name);
        System.out.println(directory.fileLength(name));
      }
    }

  }

  @Test
  public void testSmallDocs() throws IOException {

    RAMDirectory directory = new RAMDirectory();
    IndexWriterConfig conf1 = new IndexWriterConfig(Version.LUCENE_43, new WhitespaceAnalyzer(Version.LUCENE_43));
    conf1.setCodec(new Blur022Codec());
    Random random1 = new Random(1);
    IndexWriter writer1 = new IndexWriter(directory, conf1);
    for (int i = 0; i < 1000; i++) {
      writer1.addDocument(getSmallDoc(random1));
    }
    writer1.close();

    DirectoryReader reader1 = DirectoryReader.open(directory);
    int numDocs1 = reader1.numDocs();
    assertEquals(1000, numDocs1);

    // for (int i = 0; i < numDocs1; i++) {
    // System.out.println(reader1.document(i));
    // }

    IndexWriterConfig conf2 = new IndexWriterConfig(Version.LUCENE_43, new WhitespaceAnalyzer(Version.LUCENE_43));
    conf2.setCodec(new Blur022Codec(1 << 16, CompressionMode.HIGH_COMPRESSION));
    Random random2 = new Random(1);
    IndexWriter writer2 = new IndexWriter(directory, conf2);
    for (int i = 0; i < 1000; i++) {
      writer2.addDocument(getSmallDoc(random2));
    }
    writer2.close();

    DirectoryReader reader2 = DirectoryReader.open(directory);
    int numDocs2 = reader2.numDocs();
    assertEquals(2000, numDocs2);

    for (int i = 0; i < 2; i++) {

      long t1 = System.nanoTime();
      long hash1 = 0;
      long hash2 = 0;
      for (int d = 0; d < 1000; d++) {
        Document document1 = reader1.document(d);
        hash1 += document1.hashCode();
      }
      long t2 = System.nanoTime();
      for (int d = 0; d < 1000; d++) {
        Document document2 = reader2.document(d + 1000);
        hash2 += document2.hashCode();
      }
      long t3 = System.nanoTime();

      System.out.println((t3 - t2) / 1000000.0);
      System.out.println((t2 - t1) / 1000000.0);

      System.out.println("doc1 " + hash1);
      System.out.println("doc2 " + hash2);
    }

    // for (int i = 0; i < numDocs2; i++) {
    // System.out.println(reader2.document(i));
    // }

    // long fileLength = directory.fileLength("_0.fdt");

    for (String name : directory.listAll()) {
      if (name.endsWith(".fdt")) {
        System.out.println(name);
        System.out.println(directory.fileLength(name));
      }
    }
  }

  private Iterable<? extends IndexableField> getSmallDoc(Random random) {
    Document document = new Document();
    document.add(new StringField("word", getWord(random), Store.YES));
    return document;
  }

  private Iterable<? extends IndexableField> getLargeDoc(Random random) {
    Document document = new Document();
    String body = getBody(random);
    // System.out.println("body size [" + body.length() + "]");
    document.add(new TextField("body", body, Store.YES));
    return document;
  }

  private String getBody(Random random) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < WORDS; i++) {
      builder.append(getWord(random)).append(' ');
    }
    return builder.toString();
  }

  private String getWord(Random random) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      builder.append((char) (random.nextInt(26) + 'a'));
    }
    return builder.toString();
  }

}
