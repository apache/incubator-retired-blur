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
package org.apache.blur.lucene.warmup;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class IndexWarmupTest {

  private Random _random = new Random();
  private int _numberOfFields = 10;

  @Test
  public void testIndexWarmup() throws IOException {
    File file = new File("./target/tmp/indexwarmup-test");
    Directory dir = FSDirectory.open(file);

    Directory directory = new TraceableDirectory(new SlowAccessDirectory(dir));
    IndexReader indexReader = getIndexReader(directory);
    int maxSampleSize = 1000;
    int blockSize = 8192;
    long totalLookups = 0;
    for (String s : directory.listAll()) {
      if (s.endsWith(".pos") || s.endsWith(".doc") || s.endsWith(".tim")) {
        long fileLength = directory.fileLength(s);
        long maxHits = (long) Math.ceil(fileLength / (double) blockSize);
        totalLookups += maxHits;
        System.out.println("file [" + s + "] size [" + fileLength + "] maxhits [" + maxHits + "]");
      }
    }
    AtomicBoolean stop = new AtomicBoolean();
    AtomicBoolean isClosed = new AtomicBoolean();
    SlowAccessDirectory._reads.set(0);
    IndexWarmup indexWarmup = new IndexWarmup(isClosed, stop, maxSampleSize, Long.MAX_VALUE);
    long t1 = System.nanoTime();
    Map<String, List<IndexTracerResult>> sampleIndex = indexWarmup.sampleIndex(indexReader, "test");
    long sampleReads = SlowAccessDirectory._reads.get();
    SlowAccessDirectory._reads.set(0);
    long t2 = System.nanoTime();
    for (int i = 0; i < _numberOfFields; i++) {
      indexWarmup.warmFile(indexReader, sampleIndex, "test" + i, "test");
    }
    long t3 = System.nanoTime();
    System.out.println((t2 - t1) / 1000000.0 + " " + (t3 - t2) / 1000000.0);
    System.out.println(totalLookups + " " + sampleReads + " " + SlowAccessDirectory._reads.get());
  }

  @Test
  public void testIndexWarmupBitSet() throws IOException {
    File file = new File("./target/tmp/indexwarmup-test");
    Directory dir = FSDirectory.open(file);

    Directory directory = new TraceableDirectory(new SlowAccessDirectory(dir));
    IndexReader indexReader = getIndexReader(directory);
    int maxSampleSize = 1000;
    int blockSize = 8192;
    // int blockSize = 1024 * 1024;
    long totalLookups = 0;
    for (String s : directory.listAll()) {
      if (s.endsWith(".pos") || s.endsWith(".doc") || s.endsWith(".tim")) {
        long fileLength = directory.fileLength(s);
        long maxHits = (long) Math.ceil(fileLength / (double) blockSize);
        totalLookups += maxHits;
        System.out.println("file [" + s + "] size [" + fileLength + "] maxhits [" + maxHits + "]");
      }
    }
    AtomicBoolean stop = new AtomicBoolean();
    AtomicBoolean isClosed = new AtomicBoolean();
    SlowAccessDirectory._reads.set(0);
    IndexWarmup indexWarmup = new IndexWarmup(isClosed, stop, maxSampleSize, Long.MAX_VALUE);
    long t1 = System.nanoTime();
    Map<String, List<IndexTracerResult>> sampleIndex = indexWarmup.sampleIndex(indexReader, "test");
    long sampleReads = SlowAccessDirectory._reads.get();
    SlowAccessDirectory._reads.set(0);
    long t2 = System.nanoTime();
    Map<String, OpenBitSet> filePartsToWarm = new HashMap<String, OpenBitSet>();
    for (int i = 0; i < _numberOfFields; i++) {
      indexWarmup.getFilePositionsToWarm(indexReader, sampleIndex, "test" + i, "test", filePartsToWarm, blockSize);
    }
    indexWarmup.warmFile(indexReader, filePartsToWarm, "test", blockSize, 1024 * 1024);
    long t3 = System.nanoTime();

    for (Entry<String, OpenBitSet> e : filePartsToWarm.entrySet()) {
      OpenBitSet bitSet = e.getValue();
      System.out.println(bitSet.length() + " " + bitSet.cardinality());
    }

    System.out.println((t2 - t1) / 1000000.0 + " " + (t3 - t2) / 1000000.0);
    System.out.println(totalLookups + " " + sampleReads + " " + SlowAccessDirectory._reads.get());
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
    for (int i = 0; i < 20000; i++) {
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
