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
package org.apache.blur.manager.writer;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.manager.indexserver.DefaultBlurIndexWarmup;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueueReaderTest {

  private static final int TOTAL_ROWS_FOR_TESTS = 10000;
  private static final String TEST_TABLE = "test-table";
  private static final File TMPDIR = new File("./target/tmp");

  private BlurIndexSimpleWriter _writer;
  private Random random = new Random();
  private ExecutorService _service;
  private File _base;
  private Configuration _configuration;

  private SharedMergeScheduler _mergeScheduler;
  private String uuid;
  private BlurIndexCloser _closer;
  private DefaultBlurIndexWarmup _indexWarmup;

  @Before
  public void setup() throws IOException {
    TableContext.clear();
    _base = new File(TMPDIR, "QueueReaderTest");
    rmr(_base);
    _base.mkdirs();

    _mergeScheduler = new SharedMergeScheduler(1);

    _configuration = new Configuration();
    _service = Executors.newThreadPool("test", 10);
    _closer = new BlurIndexCloser();
    _indexWarmup = new DefaultBlurIndexWarmup(1000000);
  }

  private void setupWriter(Configuration configuration) throws IOException {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(TEST_TABLE);
    /*
     * if reload is set to true...we create a new writer instance pointing to
     * the same location as the old one..... so previous writer instances should
     * be closed
     */

    uuid = UUID.randomUUID().toString();

    tableDescriptor.setTableUri(new File(_base, "table-store-" + uuid).toURI().toString());
    tableDescriptor.putToTableProperties(BlurConstants.BLUR_SHARD_INDEX_QUEUE_READER_CLASS,
        QueueReaderBasicInMemory.class.getName());
    TableContext tableContext = TableContext.create(tableDescriptor);
    File path = new File(_base, "index_" + uuid);
    path.mkdirs();
    FSDirectory directory = FSDirectory.open(path);
    ShardContext shardContext = ShardContext.create(tableContext, "test-shard-" + uuid);
    _writer = new BlurIndexSimpleWriter(shardContext, directory, _mergeScheduler, _service, _closer, _indexWarmup);
  }

  @After
  public void tearDown() throws IOException {
    _writer.close();
    _mergeScheduler.close();
    _service.shutdownNow();
    rmr(_base);
  }

  private void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }

  @Test
  public void testQueueReader() throws IOException, InterruptedException {
    System.out.println(_configuration.get(BlurConstants.BLUR_SHARD_INDEX_QUEUE_READER_CLASS));
    setupWriter(_configuration);
    final String table = _writer.getShardContext().getTableContext().getTable();
    new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < TOTAL_ROWS_FOR_TESTS; i++) {
          try {
            QueueReaderBasicInMemory._queue.put(genRowMutation(table));
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    }).start();
    while (true) {
      if (_writer.getIndexSearcher().getIndexReader().numDocs() == TOTAL_ROWS_FOR_TESTS) {
        break;
      }
      Thread.sleep(100);
    }
    // YAY!!!  it worked!
  }

  private RowMutation genRowMutation(String table) {
    RowMutation rowMutation = new RowMutation();
    rowMutation.setRowId(Long.toString(random.nextLong()));
    rowMutation.setTable(table);
    rowMutation.setRowMutationType(RowMutationType.REPLACE_ROW);
    Record record = new Record();
    record.setFamily("testing");
    record.setRecordId(Long.toString(random.nextLong()));
    for (int i = 0; i < 10; i++) {
      record.addToColumns(new Column("col" + i, Long.toString(random.nextLong())));
    }
    rowMutation.addToRecordMutations(new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, record));
    return rowMutation;
  }

}
