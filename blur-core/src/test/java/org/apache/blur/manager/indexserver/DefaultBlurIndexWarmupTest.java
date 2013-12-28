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
package org.apache.blur.manager.indexserver;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.lucene.warmup.TraceableDirectory;
import org.apache.blur.manager.indexserver.DistributedIndexServer.ReleaseReader;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class DefaultBlurIndexWarmupTest {

  private Random _random = new Random();
  private int _numberOfFields = 10;

  @Test
  public void testDefaultBlurIndexWarmupTestFullIndexReader() throws IOException {
    File file = new File("./target/tmp/DefaultBlurIndexWarmupTest-test");
    Directory dir = FSDirectory.open(file);

    Directory directory = new TraceableDirectory(dir);
    IndexReader indexReader = getIndexReader(directory);

    DefaultBlurIndexWarmup indexWarmup = new DefaultBlurIndexWarmup(10000000);
    AtomicBoolean isClosed = new AtomicBoolean();

    AtomicLong pauseWarmup = new AtomicLong();
    ReleaseReader releaseReader = new ReleaseReader() {
      @Override
      public void release() throws IOException {

      }
    };
    String shard = "shard";
    TableDescriptor table = new TableDescriptor();
    table.setName("test");

    long t1 = System.nanoTime();
    indexWarmup.warmBlurIndex(table, shard, indexReader, isClosed, releaseReader, pauseWarmup);
    long t2 = System.nanoTime();
    System.out.println((t2 - t1) / 1000000.0);
  }

  @Test
  public void testDefaultBlurIndexWarmupTestOneSegment() throws IOException {
    File file = new File("./target/tmp/DefaultBlurIndexWarmupTest-test");
    Directory dir = FSDirectory.open(file);

    Directory directory = new TraceableDirectory(dir);
    IndexReader indexReader = getIndexReader(directory);

    DefaultBlurIndexWarmup indexWarmup = new DefaultBlurIndexWarmup(10000000);
    AtomicBoolean isClosed = new AtomicBoolean();

    AtomicLong pauseWarmup = new AtomicLong();
    ReleaseReader releaseReader = new ReleaseReader() {
      @Override
      public void release() throws IOException {

      }
    };
    String shard = "shard";
    TableDescriptor table = new TableDescriptor();
    table.setName("test");

    AtomicReader reader = indexReader.leaves().iterator().next().reader();

    long t1 = System.nanoTime();
    indexWarmup.warmBlurIndex(table, shard, reader, isClosed, releaseReader, pauseWarmup);
    long t2 = System.nanoTime();
    System.out.println((t2 - t1) / 1000000.0);
  }

  private IndexReader getIndexReader(Directory directory) throws IOException {
    if (!DirectoryReader.indexExists(directory)) {
      long t1 = System.nanoTime();
      populate(directory);
      long t2 = System.nanoTime();
      System.out.println((t2 - t1) / 1000000.0);
    }
    return DirectoryReader.open(directory);
  }

  private void populate(Directory directory) throws IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new StandardAnalyzer(Version.LUCENE_43));
    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    IndexWriter writer = new IndexWriter(directory, conf);
    addDocs(writer);
    writer.close();
  }

  private void addDocs(IndexWriter writer) throws IOException {
    for (int i = 0; i < 2000; i++) {
      writer.addDocument(getDoc());
    }
  }

  private Iterable<? extends IndexableField> getDoc() {
    Document document = new Document();
    document.add(new TextField(getFieldName(), getText(), Store.YES));
    return document;
  }

  private String getText() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      builder.append(getWord()).append(' ');
    }
    return builder.toString();
  }

  private String getWord() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      builder.append(getChar());
    }
    return builder.toString();
  }

  private char getChar() {
    return (char) (_random.nextInt(26) + 'a');
  }

  private String getFieldName() {
    return "test" + _random.nextInt(_numberOfFields);
  }

}
