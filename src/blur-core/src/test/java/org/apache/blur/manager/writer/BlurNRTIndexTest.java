package org.apache.blur.manager.writer;

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

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceFileGC;
import org.apache.blur.lucene.store.refcounter.IndexInputCloser;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlurNRTIndexTest {

  private static final int TEST_NUMBER_WAIT_VISIBLE = 500;
  private static final int TEST_NUMBER = 50000;

  private static final File TMPDIR = new File("./target/tmp");

  private BlurNRTIndex writer;
  private Random random = new Random();
  private ExecutorService service;
  private File base;
  private Configuration configuration;

  private DirectoryReferenceFileGC gc;
  private IndexInputCloser closer;
  private SharedMergeScheduler mergeScheduler;

  @Before
  public void setup() throws IOException {
    TableContext.clear();
    base = new File(TMPDIR, "blur-index-writer-test");
    rm(base);
    base.mkdirs();

    mergeScheduler = new SharedMergeScheduler();
    gc = new DirectoryReferenceFileGC();
    gc.init();
    closer = new IndexInputCloser();
    closer.init();

    configuration = new Configuration();
    service = Executors.newThreadPool("test", 10);
  }

  private void setupWriter(Configuration configuration, long refresh) throws IOException {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test-table");
    String uuid = UUID.randomUUID().toString();
    tableDescriptor.setTableUri(new File(base, "table-store-" + uuid).toURI().toString());
    tableDescriptor.setAnalyzerDefinition(new AnalyzerDefinition());
    tableDescriptor.putToTableProperties("blur.shard.time.between.refreshs", Long.toString(refresh));

    TableContext tableContext = TableContext.create(tableDescriptor);
    File path = new File(base, "index_" + uuid);
    path.mkdirs();
    FSDirectory directory = FSDirectory.open(path);
    ShardContext shardContext = ShardContext.create(tableContext, "test-shard-" + uuid);
    writer = new BlurNRTIndex(shardContext, mergeScheduler, closer, directory, gc, service);
  }

  @After
  public void tearDown() throws IOException {
    writer.close();
    mergeScheduler.close();
    closer.close();
    gc.close();
    service.shutdownNow();
    rm(base);
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

  @Test
  public void testBlurIndexWriter() throws IOException {
    setupWriter(configuration, 5);
    long s = System.nanoTime();
    int total = 0;
    for (int i = 0; i < TEST_NUMBER_WAIT_VISIBLE; i++) {
      writer.replaceRow(true, true, genRow());
      IndexSearcherClosable searcher = writer.getIndexReader();
      IndexReader reader = searcher.getIndexReader();
      assertEquals(i + 1, reader.numDocs());
      searcher.close();
      total++;
    }
    long e = System.nanoTime();
    double seconds = (e - s) / 1000000000.0;
    double rate = total / seconds;
    System.out.println("Rate " + rate);
    IndexSearcherClosable searcher = writer.getIndexReader();
    IndexReader reader = searcher.getIndexReader();
    assertEquals(TEST_NUMBER_WAIT_VISIBLE, reader.numDocs());
    searcher.close();
  }

  @Test
  public void testBlurIndexWriterFaster() throws IOException, InterruptedException {
    setupWriter(configuration, 100);
    IndexSearcherClosable searcher1 = writer.getIndexReader();
    IndexReader reader1 = searcher1.getIndexReader();
    assertEquals(0, reader1.numDocs());
    searcher1.close();
    long s = System.nanoTime();
    int total = 0;
    for (int i = 0; i < TEST_NUMBER; i++) {
      if (i == TEST_NUMBER - 1) {
        writer.replaceRow(true, true, genRow());
      } else {
        writer.replaceRow(false, true, genRow());
      }
      total++;
    }
    long e = System.nanoTime();
    double seconds = (e - s) / 1000000000.0;
    double rate = total / seconds;
    System.out.println("Rate " + rate);
    // //wait one second for the data to become visible the test is set to
    // refresh once every 25 ms
    // Thread.sleep(1000);
    writer.refresh();
    IndexSearcherClosable searcher2 = writer.getIndexReader();
    IndexReader reader2 = searcher2.getIndexReader();
    assertEquals(TEST_NUMBER, reader2.numDocs());
    searcher2.close();
  }

  private Row genRow() {
    Row row = new Row();
    row.setId(Long.toString(random.nextLong()));
    Record record = new Record();
    record.setFamily("testing");
    record.setRecordId(Long.toString(random.nextLong()));
    for (int i = 0; i < 10; i++) {
      record.addToColumns(new Column("col" + i, Long.toString(random.nextLong())));
    }
    row.addToRecords(record);
    return row;
  }

}
