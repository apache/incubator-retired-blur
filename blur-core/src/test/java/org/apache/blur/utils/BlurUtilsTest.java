package org.apache.blur.utils;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.blur.thrift.generated.Selector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

public class BlurUtilsTest {
  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "/tmp"));

  @Test
  public void testHumanizeTime1() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(42) + TimeUnit.SECONDS.toMillis(37)
        + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 42 minutes 37 seconds", humanizeTime);
  }

  @Test
  public void testHumanizeTime2() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(42) + TimeUnit.SECONDS.toMillis(37)
        + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("42 minutes 37 seconds", humanizeTime);
  }

  @Test
  public void testHumanizeTime3() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(37)
        + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 0 minutes 37 seconds", humanizeTime);
  }

  @Test
  public void testHumanizeTime4() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(0)
        + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 0 minutes 0 seconds", humanizeTime);
  }

  @Test
  public void testHumanizeTime5() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(37)
        + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("37 seconds", humanizeTime);
  }

  @Test
  public void testHumanizeTime6() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(0)
        + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("0 seconds", humanizeTime);
  }

  @Test
  public void testValidateShardCount() throws IOException {
    File file = new File(TMPDIR, "ValidateShardCount-test");
    rm(file);
    Path path = new Path(file.toURI());
    Configuration conf = new Configuration();
    FileSystem fileSystem = path.getFileSystem(conf);
    fileSystem.mkdirs(path);
    int shardCount = 10;
    createShardDirs(shardCount, fileSystem, path);
    BlurUtil.validateShardCount(shardCount, fileSystem, path);
  }

  @Test
  public void testValidateShardCountExtraDir() throws IOException {
    File file = new File(TMPDIR, "ValidateShardCount-test");
    rm(file);
    Path path = new Path(file.toURI());
    Configuration conf = new Configuration();
    FileSystem fileSystem = path.getFileSystem(conf);
    fileSystem.mkdirs(path);
    int shardCount = 10;
    createShardDirs(shardCount, fileSystem, path);
    fileSystem.mkdirs(new Path(path, "logs"));
    BlurUtil.validateShardCount(shardCount, fileSystem, path);
  }

  @Test
  public void testValidateShardCountTooFew() throws IOException {
    File file = new File(TMPDIR, "ValidateShardCount-test");
    rm(file);
    Path path = new Path(file.toURI());
    Configuration conf = new Configuration();
    FileSystem fileSystem = path.getFileSystem(conf);
    fileSystem.mkdirs(path);
    int shardCount = 10;
    createShardDirs(shardCount - 1, fileSystem, path);
    try {
      BlurUtil.validateShardCount(shardCount, fileSystem, path);
      fail();
    } catch (Exception e) {
      // Should throw exception
    }
  }

  @Test
  public void testValidateShardCountTooMany() throws IOException {
    File file = new File(TMPDIR, "ValidateShardCount-test");
    rm(file);
    Path path = new Path(file.toURI());
    Configuration conf = new Configuration();
    FileSystem fileSystem = path.getFileSystem(conf);
    fileSystem.mkdirs(path);
    int shardCount = 10;
    createShardDirs(shardCount + 1, fileSystem, path);
    try {
      BlurUtil.validateShardCount(shardCount, fileSystem, path);
      fail();
    } catch (Exception e) {
      // Should throw exception
    }
  }

  @Test
  public void testFetchDocuments() throws CorruptIndexException, LockObtainFailedException, IOException {
    Selector selector = new Selector();
    selector.setLocationId("shard/0");
    Set<String> columnFamiliesToFetch = new HashSet<String>();
    columnFamiliesToFetch.add("f1");
    columnFamiliesToFetch.add("f2");
    selector.setColumnFamiliesToFetch(columnFamiliesToFetch);

    ResetableDocumentStoredFieldVisitor resetableDocumentStoredFieldVisitor = new ResetableDocumentStoredFieldVisitor();
    // List<Document> docs = BlurUtil.fetchDocuments(getReader(), new
    // Term("a","b"), resetableDocumentStoredFieldVisitor, selector, 10000000,
    // "test-context", new
    // Term(BlurConstants.PRIME_DOC,BlurConstants.PRIME_DOC_VALUE));
    AtomicBoolean moreDocsToFetch = new AtomicBoolean(false);
    AtomicInteger totalRecords = new AtomicInteger();
    List<Document> docs = BlurUtil.fetchDocuments(getReader(), resetableDocumentStoredFieldVisitor, selector, 10000000,
        "test-context", new Term(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE), null, moreDocsToFetch,
        totalRecords, null);
    assertEquals(docs.size(), 1);
    assertFalse(moreDocsToFetch.get());
    assertEquals(1, totalRecords.get());
  }

  @Test
  public void testFetchDocumentsStrictFamilyOrder() throws CorruptIndexException, LockObtainFailedException,
      IOException {
    Selector selector = new Selector();
    selector.setLocationId("shard/0");
    Set<String> columnFamiliesToFetch = new HashSet<String>();
    columnFamiliesToFetch.add("f1");
    columnFamiliesToFetch.add("f2");
    selector.setColumnFamiliesToFetch(columnFamiliesToFetch);
    selector.addToOrderOfFamiliesToFetch("f1");
    selector.addToOrderOfFamiliesToFetch("f2");

    ResetableDocumentStoredFieldVisitor resetableDocumentStoredFieldVisitor = new ResetableDocumentStoredFieldVisitor();
    AtomicBoolean moreDocsToFetch = new AtomicBoolean(false);
    AtomicInteger totalRecords = new AtomicInteger();
    List<Document> docs = BlurUtil.fetchDocuments(getReaderWithDocsHavingFamily(), resetableDocumentStoredFieldVisitor,
        selector, 10000000, "test-context", new Term(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE), null,
        moreDocsToFetch, totalRecords, null);
    assertEquals(docs.size(), 2);
    assertEquals(docs.get(0).getField("family").stringValue(), "f1");
    assertEquals(docs.get(1).getField("family").stringValue(), "f2");
    assertFalse(moreDocsToFetch.get());
    assertEquals(2, totalRecords.get());
  }

  @Test
  public void testFetchDocumentsWithoutFamily() throws CorruptIndexException, LockObtainFailedException, IOException {
    Selector selector = new Selector();
    selector.setLocationId("shard/0");
    ResetableDocumentStoredFieldVisitor resetableDocumentStoredFieldVisitor = new ResetableDocumentStoredFieldVisitor();
    AtomicBoolean moreDocsToFetch = new AtomicBoolean(false);
    AtomicInteger totalRecords = new AtomicInteger();
    List<Document> docs = BlurUtil.fetchDocuments(getReader(), resetableDocumentStoredFieldVisitor, selector, 10000000,
        "test-context", new Term(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE), null, moreDocsToFetch,
        totalRecords, null);
    assertEquals(docs.size(), 2);
    assertFalse(moreDocsToFetch.get());
    assertEquals(2, totalRecords.get());
  }

  private void rm(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }

  private void createShardDirs(int shardCount, FileSystem fileSystem, Path path) throws IOException {
    for (int i = 0; i < shardCount; i++) {
      fileSystem.mkdirs(new Path(path, ShardUtil.getShardName(BlurConstants.SHARD_PREFIX, i)));
    }
  }

  private IndexReader getReader() throws CorruptIndexException, LockObtainFailedException, IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
    doc.add(new StringField("a", "b", Store.YES));
    doc.add(new StringField("family", "f1", Store.YES));

    Document doc1 = new Document();
    doc.add(new StringField("a", "b", Store.YES));
    writer.addDocument(doc);
    writer.addDocument(doc1);
    writer.close();
    return DirectoryReader.open(directory);
  }

  private IndexReader getReaderWithDocsHavingFamily() throws CorruptIndexException, LockObtainFailedException,
      IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
    doc.add(new StringField("a", "b", Store.YES));
    doc.add(new StringField("family", "f2", Store.YES));

    Document doc1 = new Document();
    doc1.add(new StringField("a", "b", Store.YES));
    doc1.add(new StringField("family", "f1", Store.YES));
    writer.addDocument(doc);
    writer.addDocument(doc1);
    writer.close();
    return DirectoryReader.open(directory);
  }

}
