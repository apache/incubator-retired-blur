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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.blur.lucene.LuceneConstant;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

public class BlurUtilsTest {

  @Test
  public void testHumanizeTime1() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(42) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 42 minutes 37 seconds", humanizeTime);
  }

  @Test
  public void testHumanizeTime2() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(42) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("42 minutes 37 seconds", humanizeTime);
  }

  @Test
  public void testHumanizeTime3() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 0 minutes 37 seconds", humanizeTime);
  }

  @Test
  public void testHumanizeTime4() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(0) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 0 minutes 0 seconds", humanizeTime);
  }

  @Test
  public void testHumanizeTime5() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("37 seconds", humanizeTime);
  }

  @Test
  public void testHumanizeTime6() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(0) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("0 seconds", humanizeTime);
  }

  @Test
  public void testMemoryUsage() throws CorruptIndexException, LockObtainFailedException, IOException {
    IndexReader reader = getReader();
    long memoryUsage = BlurUtil.getMemoryUsage(reader);
    assertTrue(memoryUsage > 0);
  }

  @Test
  public void testRecordMatch() {
    Record r1 = BlurUtil.newRecord("test-family", "record-1", BlurUtil.newColumn("a", "b"));
    Record r2 = BlurUtil.newRecord("test-family", "record-1", BlurUtil.newColumn("c", "d"));
    Record r3 = BlurUtil.newRecord("test-family", "record-2", BlurUtil.newColumn("e", "f"));
    Record r4 = BlurUtil.newRecord("test-family-2", "record-1", BlurUtil.newColumn("g", "h"));

    assertTrue("should match with same family and record-id", BlurUtil.match(r1, r2));
    assertFalse("should not match with different record-id", BlurUtil.match(r1, r3));
    assertFalse("should not match with different family", BlurUtil.match(r1, r4));
  }

  @Test
  public void testRecordMutationMatch() {
    RecordMutation rm1 = BlurUtil.newRecordMutation("test-family", "record-1", BlurUtil.newColumn("a", "b"));
    RecordMutation rm2 = BlurUtil.newRecordMutation("test-family", "record-2", BlurUtil.newColumn("c", "d"));
    RecordMutation rm3 = BlurUtil.newRecordMutation("test-family-2", "record-1", BlurUtil.newColumn("e", "f"));
    Record r = BlurUtil.newRecord("test-family", "record-1", BlurUtil.newColumn("g", "h"));

    assertTrue("should match with same family and record-id", BlurUtil.match(rm1, r));
    assertFalse("should not match with different record-id", BlurUtil.match(rm2, r));
    assertFalse("should not match with different family", BlurUtil.match(rm3, r));
  }

  @Test
  public void testFindRecordMutation() {
    RecordMutation rm1 = BlurUtil.newRecordMutation("test-family", "record-1", BlurUtil.newColumn("a", "b"));
    RecordMutation rm2 = BlurUtil.newRecordMutation("test-family", "record-2", BlurUtil.newColumn("c", "d"));
    RecordMutation rm3 = BlurUtil.newRecordMutation("test-family-2", "record-1", BlurUtil.newColumn("e", "f"));
    RowMutation row = BlurUtil.newRowMutation("test-table", "row-123", rm1, rm2, rm3);
    Record r = BlurUtil.newRecord("test-family", "record-2", BlurUtil.newColumn("g", "h"));
    Record r2 = BlurUtil.newRecord("test-family", "record-99", BlurUtil.newColumn("g", "h"));

    assertEquals("should find record-2", rm2, BlurUtil.findRecordMutation(row, r));
    assertNull("should not find record-99", BlurUtil.findRecordMutation(row, r2));
  }

  @Test
  public void testValidateShardCount() throws IOException {
    File file = new File("./tmp/ValidateShardCount-test");
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
    File file = new File("./tmp/ValidateShardCount-test");
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
    File file = new File("./tmp/ValidateShardCount-test");
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
    File file = new File("./tmp/ValidateShardCount-test");
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
      fileSystem.mkdirs(new Path(path, BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, i)));
    }
  }

  private IndexReader getReader() throws CorruptIndexException, LockObtainFailedException, IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(LuceneConstant.LUCENE_VERSION, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new Field("a", "b", Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    writer.addDocument(doc);
    writer.close();
    return IndexReader.open(directory);
  }

}
