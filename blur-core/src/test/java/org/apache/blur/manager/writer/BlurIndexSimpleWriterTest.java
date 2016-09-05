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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.concurrent.Executors;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.store.hdfs.BlurLockFactory;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.trace.BaseTraceStorage;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.TraceCollector;
import org.apache.blur.trace.TraceStorage;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.IndexReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlurIndexSimpleWriterTest {

  private static final int TOTAL_ROWS_FOR_TESTS = 10000;
  private static final String TEST_TABLE = "test-table";
  private static final int TEST_NUMBER_WAIT_VISIBLE = 100;
  private static final int TEST_NUMBER = 50000;

  private static final File TMPDIR = new File("./target/tmp");

  private BlurIndexSimpleWriter _writer;
  private Random random = new Random();
  private ExecutorService _service;
  private File _base;
  private Configuration _configuration;

  private SharedMergeScheduler _mergeScheduler;
  private String uuid;
  private BlurIndexCloser _closer;
  private Timer _indexImporterTimer;
  private Timer _bulkTimer;
  private Timer _idleWriterTimer;

  @Before
  public void setup() throws IOException {
    _indexImporterTimer = new Timer("Index Importer", true);
    _bulkTimer = new Timer("Bulk Indexing", true);
    _idleWriterTimer = new Timer("Idle Writer", true);
    TableContext.clear();
    _base = new File(TMPDIR, "blur-index-writer-test");
    rmr(_base);
    _base.mkdirs();

    _mergeScheduler = new SharedMergeScheduler(1);

    _configuration = new Configuration();
    _service = Executors.newThreadPool("test", 10);
    _closer = new BlurIndexCloser();
  }

  private void setupWriter(Configuration configuration) throws IOException {
    setupWriter(configuration, false);
  }

  private void setupWriter(Configuration configuration, boolean reload) throws IOException {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(TEST_TABLE);
    if (!reload && uuid == null) {
      uuid = UUID.randomUUID().toString();
    }
    tableDescriptor.setTableUri(new File(_base, "table-store-" + uuid).toURI().toString());
    Map<String, String> tableProperties = new HashMap<String, String>();
    tableProperties.put(BlurConstants.BLUR_SHARD_QUEUE_MAX_PAUSE_TIME_WHEN_EMPTY, "500");
    tableProperties.put(BlurConstants.BLUR_SHARD_QUEUE_MAX_QUEUE_BATCH_SIZE, "500");
    tableProperties.put(BlurConstants.BLUR_SHARD_QUEUE_MAX_WRITER_LOCK_TIME, "1000");
    tableProperties.put(BlurConstants.BLUR_SHARD_QUEUE_MAX_INMEMORY_LENGTH, "1000");

    tableDescriptor.setTableProperties(tableProperties);
    TableContext tableContext = TableContext.create(tableDescriptor);
    File path = new File(_base, "index_" + uuid);
    path.mkdirs();

    Path hdfsPath = new Path(path.toURI());
    HdfsDirectory directory = new HdfsDirectory(_configuration, hdfsPath);
    BlurLockFactory lockFactory = new BlurLockFactory(_configuration, hdfsPath, "unit-test", BlurUtil.getPid());
    directory.setLockFactory(lockFactory);

    ShardContext shardContext = ShardContext.create(tableContext, "test-shard-" + uuid);
    _writer = new BlurIndexSimpleWriter(new BlurIndexConfig(shardContext, directory, _mergeScheduler, _service, _closer,
        _indexImporterTimer, _bulkTimer, null, _idleWriterTimer, TimeUnit.SECONDS.toMillis(5)));
  }

  @After
  public void tearDown() throws IOException {
    _indexImporterTimer.cancel();
    _indexImporterTimer.purge();
    _bulkTimer.cancel();
    _bulkTimer.purge();
    _idleWriterTimer.cancel();
    _idleWriterTimer.purge();

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
  public void testRollbackAndReopen() throws IOException {
    setupWriter(_configuration);
    {
      IndexSearcherCloseable searcher = _writer.getIndexSearcher();
      IndexReader reader = searcher.getIndexReader();
      assertEquals(0, reader.numDocs());
      searcher.close();
    }

    MutatableAction action = new MutatableAction(_writer.getShardContext());
    action.replaceRow(new Row());
    try {
      _writer.process(action);
      fail("should throw exception");
    } catch (IOException e) {
      // do nothing
    }
    {
      IndexSearcherCloseable searcher = _writer.getIndexSearcher();
      IndexReader reader = searcher.getIndexReader();
      assertEquals(0, reader.numDocs());
      searcher.close();
    }

    action.replaceRow(genRow());
    _writer.process(action);

    {
      IndexSearcherCloseable searcher = _writer.getIndexSearcher();
      IndexReader reader = searcher.getIndexReader();
      assertEquals(1, reader.numDocs());
      searcher.close();
    }
  }

  @Test
  public void testBlurIndexWriter() throws IOException {
    setupWriter(_configuration);
    long s = System.nanoTime();
    int total = 0;
    TraceStorage oldStorage = Trace.getStorage();
    Trace.setStorage(new BaseTraceStorage(new BlurConfiguration()) {
      @Override
      public void close() throws IOException {

      }

      @Override
      public void store(TraceCollector collector) {
        // try {
        // System.out.println(collector.toJsonObject().toString(1));
        // } catch (JSONException e) {
        // e.printStackTrace();
        // }
      }
    });
    Trace.setupTrace("test");
    for (int i = 0; i < TEST_NUMBER_WAIT_VISIBLE; i++) {
      MutatableAction action = new MutatableAction(_writer.getShardContext());
      action.replaceRow(genRow());
      _writer.process(action);
      IndexSearcherCloseable searcher = _writer.getIndexSearcher();
      IndexReader reader = searcher.getIndexReader();
      assertEquals(i + 1, reader.numDocs());
      searcher.close();
      total++;
      int readersToBeClosedCount = _closer.getReadersToBeClosedCount();
      int readerGenerationCount = _writer.getReaderGenerationCount();
      assertTrue((readerGenerationCount - readersToBeClosedCount) < 3);
    }
    Trace.tearDownTrace();
    long e = System.nanoTime();
    double seconds = (e - s) / 1000000000.0;
    double rate = total / seconds;
    System.out.println("Rate " + rate);
    IndexSearcherCloseable searcher = _writer.getIndexSearcher();
    IndexReader reader = searcher.getIndexReader();
    assertEquals(TEST_NUMBER_WAIT_VISIBLE, reader.numDocs());
    searcher.close();
    Trace.setStorage(oldStorage);
  }

  @Test
  public void testBlurIndexWriterFaster() throws IOException, InterruptedException {
    setupWriter(_configuration);
    IndexSearcherCloseable searcher1 = _writer.getIndexSearcher();
    IndexReader reader1 = searcher1.getIndexReader();
    assertEquals(0, reader1.numDocs());
    searcher1.close();
    long s = System.nanoTime();
    int total = 0;
    MutatableAction action = new MutatableAction(_writer.getShardContext());
    for (int i = 0; i < TEST_NUMBER; i++) {
      action.replaceRow(genRow());
      total++;
    }
    _writer.process(action);
    long e = System.nanoTime();
    double seconds = (e - s) / 1000000000.0;
    double rate = total / seconds;
    System.out.println("Rate " + rate);
    // //wait one second for the data to become visible the test is set to
    // refresh once every 25 ms
    Thread.sleep(1000);// Hack for now
    IndexSearcherCloseable searcher2 = _writer.getIndexSearcher();
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

  @Test
  public void testCreateSnapshot() throws IOException {
    setupWriter(_configuration);
    _writer.createSnapshot("test_snapshot");
    assertTrue(_writer.getSnapshots().contains("test_snapshot"));

    // check that the file is persisted
    Path snapshotsDirPath = _writer.getSnapshotsDirectoryPath();
    FileSystem fileSystem = snapshotsDirPath.getFileSystem(_configuration);
    assertTrue(fileSystem.exists(new Path(snapshotsDirPath, "000000000001")));

    // create a new writer instance and test whether the snapshots are loaded
    // properly
    _writer.close();
    setupWriter(_configuration, true);
    assertTrue(_writer.getSnapshots().contains("test_snapshot"));
  }

  @Test
  public void testRemoveSnapshots() throws IOException {
    setupWriter(_configuration);
    Path snapshotsDirPath = _writer.getSnapshotsDirectoryPath();
    FileSystem fileSystem = snapshotsDirPath.getFileSystem(new Configuration());
    fileSystem.mkdirs(snapshotsDirPath);

    _writer.createSnapshot("test_snapshot1");
    _writer.createSnapshot("test_snapshot2");

    // re-load the writer to load the snpshots
    _writer.close();
    setupWriter(_configuration, true);
    assertEquals(2, _writer.getSnapshots().size());

    _writer.removeSnapshot("test_snapshot2");
    assertEquals(1, _writer.getSnapshots().size());
    assertTrue(!_writer.getSnapshots().contains("test_snapshot2"));
  }

  @Test
  public void testEnqueue() throws IOException, InterruptedException {
    setupWriter(_configuration);
    runQueueTest(TOTAL_ROWS_FOR_TESTS, TOTAL_ROWS_FOR_TESTS);
    runQueueTest(TOTAL_ROWS_FOR_TESTS, TOTAL_ROWS_FOR_TESTS * 2);
    runQueueTest(TOTAL_ROWS_FOR_TESTS, TOTAL_ROWS_FOR_TESTS * 3);
  }

  @Test
  public void testAutoCloseOfWriter() throws InterruptedException, IOException {
    setupWriter(_configuration);
    Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    for (int i = 0; i < 10; i++) {
      if (_writer.isWriterClosed()) {
        return;
      }
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
    fail();
  }

  private void runQueueTest(final int mutatesToAdd, int numberOfValidDocs) throws IOException, InterruptedException {
    final String table = _writer.getShardContext().getTableContext().getTable();
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < mutatesToAdd; i++) {
          try {
            _writer.enqueue(Arrays.asList(genRowMutation(table)));
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    });
    thread.start();
    long start = System.currentTimeMillis();
    while (true) {
      if (_writer.getIndexSearcher().getIndexReader().numDocs() == numberOfValidDocs) {
        long end = System.currentTimeMillis();
        System.out.println("[" + TOTAL_ROWS_FOR_TESTS + "] Mutations in [" + (end - start) + " ms]");
        break;
      }
      Thread.sleep(100);
    }
    thread.join();
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
