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
import java.util.concurrent.ExecutorService;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceFileGC;
import org.apache.blur.lucene.store.refcounter.IndexInputCloser;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlurIndexReaderTest {

  private static final File TMPDIR = new File("./target/tmp");

  private BlurNRTIndex writer;
  private Random random = new Random();
  private ExecutorService service;
  private File base;
  private Configuration configuration;

  private DirectoryReferenceFileGC gc;
  private IndexInputCloser closer;
  private SharedMergeScheduler mergeScheduler;
  private BlurIndexReader reader;

  @Before
  public void setup() throws IOException {
    TableContext.clear();
    base = new File(TMPDIR, "blur-index-reader-test");
    rm(base);
    base.mkdirs();

    mergeScheduler = new SharedMergeScheduler();
    gc = new DirectoryReferenceFileGC();
    gc.init();
    closer = new IndexInputCloser();
    closer.init();

    configuration = new Configuration();
    service = Executors.newThreadPool("test", 1);
    
  }

  private void setupWriter(Configuration configuration, long refresh) throws IOException {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test-table");
    tableDescriptor.setTableUri(new File(base, "table-store").toURI().toString());
    tableDescriptor.setAnalyzerDefinition(new AnalyzerDefinition());
    tableDescriptor.putToTableProperties("blur.shard.time.between.refreshs", Long.toString(refresh));
    tableDescriptor.putToTableProperties("blur.shard.time.between.commits", Long.toString(1000));
    
    TableContext tableContext = TableContext.create(tableDescriptor);
    FSDirectory directory = FSDirectory.open(new File(base, "index"));

    ShardContext shardContext = ShardContext.create(tableContext, "test-shard");

    writer = new BlurNRTIndex(shardContext, mergeScheduler, closer, directory, gc, service);
    BlurIndexRefresher refresher = new BlurIndexRefresher();
    BlurIndexCloser indexCloser = new BlurIndexCloser();
    refresher.init();
    indexCloser.init();
    reader = new BlurIndexReader(shardContext, directory, refresher, indexCloser);
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
  public void testBlurIndexWriter() throws IOException, InterruptedException {
    setupWriter(configuration, 1);
    IndexSearcher searcher = reader.getSearcher();
    writer.replaceRow(true, true, genRow());
    Thread.sleep(1500);
    assertEquals(0,searcher.getIndexReader().numDocs());
    reader.refresh();
    assertEquals(1,reader.getSearcher().getIndexReader().numDocs());
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
