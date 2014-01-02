package org.apache.blur.thrift;

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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.MiniCluster;
import org.apache.blur.TestType;
import org.apache.blur.analysis.FieldManager;
import org.apache.blur.manager.IndexManager;
import org.apache.blur.server.TableContext;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.thrift.generated.Facet;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.util.BlurThriftHelper;
import org.apache.blur.utils.GCWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlurClusterTest {

  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_BlurClusterTest"));
  private static MiniCluster miniCluster;

  private int numberOfDocs = 1000;

  @BeforeClass
  public static void startCluster() throws IOException {
    GCWatcher.init(0.60);
    LocalFileSystem localFS = FileSystem.getLocal(new Configuration());
    File testDirectory = new File(TMPDIR, "blur-cluster-test").getAbsoluteFile();
    testDirectory.mkdirs();

    Path directory = new Path(testDirectory.getPath());
    FsPermission dirPermissions = localFS.getFileStatus(directory).getPermission();
    FsAction userAction = dirPermissions.getUserAction();
    FsAction groupAction = dirPermissions.getGroupAction();
    FsAction otherAction = dirPermissions.getOtherAction();

    StringBuilder builder = new StringBuilder();
    builder.append(userAction.ordinal());
    builder.append(groupAction.ordinal());
    builder.append(otherAction.ordinal());
    String dirPermissionNum = builder.toString();
    System.setProperty("dfs.datanode.data.dir.perm", dirPermissionNum);
    testDirectory.delete();
    miniCluster = new MiniCluster();
    miniCluster.startBlurCluster(new File(testDirectory, "cluster").getAbsolutePath(), 2, 3, true);
  }

  @AfterClass
  public static void shutdownCluster() {
    miniCluster.shutdownBlurCluster();
  }

  private Iface getClient() {
    return BlurClient.getClient(miniCluster.getControllerConnectionStr());
  }

  @Test
  public void runClusterIntegrationTests() throws BlurException, TException, IOException, InterruptedException,
      KeeperException {
    start("testCreateTable");
    testCreateTable();
    start("testLoadTable");
    testLoadTable();
    start("testForEmptySchema");
    testForEmptySchema();
    start("testQueryWithSelector");
    testQueryWithSelector();
    start("testQueryWithFacets");
    testQueryWithFacets();
    start("testBatchFetch");
    testBatchFetch();
    start("testLoadTable");
    testQueryCancel();
    start("testBackPressureViaQuery");
    testBackPressureViaQuery();
    start("testTestShardFailover");
    testTestShardFailover();
    start("testTermsList");
    testTermsList();
    start("testCreateDisableAndRemoveTable");
    testCreateDisableAndRemoveTable();
    start("testCreateTableWithCustomType");
    testCreateTableWithCustomType();
  }

  private void start(String name) {
    System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
    System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
    System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
    System.out.println("Staring [" + name + "]");
  }

  private void testForEmptySchema() throws BlurException, TException, IOException {
    Blur.Iface client = getClient();
    Schema schema = client.schema("test");
    Map<String, Map<String, ColumnDefinition>> families = schema.getFamilies();
    assertTrue(!families.isEmpty());
    int size = families.size();
    System.out.println(size);

    TableContext tableContext = TableContext.create(client.describe("test"));
    FieldManager fieldManager = tableContext.getFieldManager();

    assertTrue(fieldManager.addColumnDefinition("test-family", "test-column", null, false, "string", null));

    TableContext.clear();
    Schema newschema = client.schema("test");
    Map<String, Map<String, ColumnDefinition>> newfamilies = newschema.getFamilies();
    assertTrue(!newfamilies.isEmpty());
    int newsize = newfamilies.size();
    assertEquals(size + 1, newsize);
  }

  private void testCreateTableWithCustomType() throws IOException, BlurException, TException {
    Blur.Iface client = getClient();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test_type");
    tableDescriptor.setShardCount(1);
    tableDescriptor.setTableUri(miniCluster.getFileSystemUri().toString() + "/blur/test_type");
    client.createTable(tableDescriptor);
    List<String> tableList = client.tableList();
    assertTrue(tableList.contains("test_type"));

    client.disableTable("test_type");

    client.enableTable("test_type");

    TableDescriptor describe = client.describe("test_type");
    Map<String, String> tableProperties = describe.getTableProperties();
    assertEquals(TestType.class.getName(), tableProperties.get("blur.fieldtype.customtype1"));
  }

  public void testCreateTable() throws BlurException, TException, IOException {
    Blur.Iface client = getClient();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test");
    tableDescriptor.setShardCount(5);
    tableDescriptor.setTableUri(miniCluster.getFileSystemUri().toString() + "/blur/test");
    client.createTable(tableDescriptor);
    List<String> tableList = client.tableList();
    assertEquals(Arrays.asList("test"), tableList);
  }

  public void testLoadTable() throws BlurException, TException, InterruptedException {
    Iface client = getClient();
    int maxFacetValue = 100;
    List<RowMutation> mutations = new ArrayList<RowMutation>();
    Random random = new Random(1);
    for (int i = 0; i < numberOfDocs; i++) {
      String rowId = UUID.randomUUID().toString();
      RecordMutation mutation = BlurThriftHelper.newRecordMutation("test", rowId,
          BlurThriftHelper.newColumn("test", "value"),
          BlurThriftHelper.newColumn("facet", Integer.toString(random.nextInt(maxFacetValue))));
      RowMutation rowMutation = BlurThriftHelper.newRowMutation("test", rowId, mutation);
      rowMutation.setWaitToBeVisible(true);
      mutations.add(rowMutation);
    }
    long s = System.nanoTime();
    client.mutateBatch(mutations);
    long e = System.nanoTime();
    System.out.println("mutateBatch took [" + (e - s) / 1000000.0 + "]");
    BlurQuery blurQueryRow = new BlurQuery();
    Query queryRow = new Query();
    queryRow.setQuery("test.test:value");
    blurQueryRow.setQuery(queryRow);
    blurQueryRow.setUseCacheIfPresent(false);
    blurQueryRow.setCacheResult(false);
    BlurResults resultsRow = client.query("test", blurQueryRow);
    assertRowResults(resultsRow);
    assertEquals(numberOfDocs, resultsRow.getTotalResults());

    BlurQuery blurQueryRecord = new BlurQuery();
    Query queryRecord = new Query();
    queryRecord.rowQuery = false;
    queryRecord.setQuery("test.test:value");
    blurQueryRecord.setQuery(queryRecord);
    BlurResults resultsRecord = client.query("test", blurQueryRecord);
    assertRecordResults(resultsRecord);
    assertEquals(numberOfDocs, resultsRecord.getTotalResults());

    Schema schema = client.schema("test");
    assertFalse(schema.getFamilies().isEmpty());
  }

  private void testQueryWithSelector() throws BlurException, TException {
    Iface client = getClient();
    BlurQuery blurQueryRow = new BlurQuery();
    Query queryRow = new Query();
    queryRow.setQuery("test.test:value");
    blurQueryRow.setQuery(queryRow);
    blurQueryRow.setUseCacheIfPresent(false);
    blurQueryRow.setCacheResult(false);
    blurQueryRow.setSelector(new Selector());

    BlurResults resultsRow = client.query("test", blurQueryRow);
    // assertRowResults(resultsRow);
    assertEquals(numberOfDocs, resultsRow.getTotalResults());

    for (BlurResult blurResult : resultsRow.getResults()) {
      System.out.println(blurResult);
    }

  }

  private void testQueryWithFacets() throws BlurException, TException {
    Iface client = getClient();
    BlurQuery blurQueryRow = new BlurQuery();
    Query queryRow = new Query();
    // queryRow.setQuery("test.test:value");
    queryRow.setQuery("*");
    blurQueryRow.setQuery(queryRow);
    blurQueryRow.setUseCacheIfPresent(false);
    blurQueryRow.setCacheResult(false);
    blurQueryRow.setSelector(new Selector());
    for (int i = 0; i < 250; i++) {
      blurQueryRow.addToFacets(new Facet("test.facet:" + i, Long.MAX_VALUE));
    }

    BlurResults resultsRow = client.query("test", blurQueryRow);
    // assertRowResults(resultsRow);
    assertEquals(numberOfDocs, resultsRow.getTotalResults());

    System.out.println(resultsRow.getFacetCounts());

    System.out.println();

  }

  public void testBatchFetch() throws BlurException, TException {
    final Iface client = getClient();
    List<String> terms = client.terms("test", null, "rowid", "", (short) 100);

    List<Selector> selectors = new ArrayList<Selector>();
    for (String s : terms) {
      Selector selector = new Selector();
      selector.setRowId(s);
      selectors.add(selector);
    }

    List<FetchResult> fetchRowBatch = client.fetchRowBatch("test", selectors);
    assertEquals(100, fetchRowBatch.size());

    int i = 0;
    for (FetchResult fetchResult : fetchRowBatch) {
      assertEquals(terms.get(i), fetchResult.getRowResult().getRow().getId());
      i++;
    }

  }

  public void testQueryCancel() throws BlurException, TException, InterruptedException {
    // This will make each collect in the collectors pause 250 ms per collect
    // call
    IndexManager.DEBUG_RUN_SLOW.set(true);

    final Iface client = getClient();
    final BlurQuery blurQueryRow = new BlurQuery();
    Query queryRow = new Query();
    queryRow.setQuery("test.test:value");
    blurQueryRow.setQuery(queryRow);
    blurQueryRow.setUseCacheIfPresent(false);
    blurQueryRow.setCacheResult(false);
    blurQueryRow.setUuid("1234");

    final AtomicReference<BlurException> error = new AtomicReference<BlurException>();
    final AtomicBoolean fail = new AtomicBoolean();

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // This call will take several seconds to execute.
          client.query("test", blurQueryRow);
          fail.set(true);
        } catch (BlurException e) {
          error.set(e);
        } catch (TException e) {
          e.printStackTrace();
          fail.set(true);
        }
      }
    }).start();
    Thread.sleep(500);
    client.cancelQuery("test", blurQueryRow.getUuid());
    BlurException blurException = pollForError(error, 10, TimeUnit.SECONDS, null, fail, -1);
    if (fail.get()) {
      fail("Unknown error, failing test.");
    }
    assertEquals(blurException.getErrorType(), ErrorType.QUERY_CANCEL);
  }

  public void testBackPressureViaQuery() throws BlurException, TException, InterruptedException {
    // This will make each collect in the collectors pause 250 ms per collect
    // call
    IndexManager.DEBUG_RUN_SLOW.set(true);
    runBackPressureViaQuery();
    Thread.sleep(1000);
    System.gc();
    System.gc();
    Thread.sleep(1000);
  }

  private void runBackPressureViaQuery() throws InterruptedException {
    final Iface client = getClient();
    final BlurQuery blurQueryRow = new BlurQuery();
    Query queryRow = new Query();
    queryRow.setQuery("test.test:value");
    blurQueryRow.setQuery(queryRow);
    blurQueryRow.setUseCacheIfPresent(false);
    blurQueryRow.setCacheResult(false);
    blurQueryRow.setUuid("1234");

    final AtomicReference<BlurException> error = new AtomicReference<BlurException>();
    final AtomicBoolean fail = new AtomicBoolean();

    System.gc();
    System.gc();
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage usage = memoryMXBean.getHeapMemoryUsage();
    long max = usage.getMax();
    System.out.println("Max Heap [" + max + "]");
    long used = usage.getUsed();
    System.out.println("Used Heap [" + used + "]");
    long limit = (long) (max * 0.80);
    System.out.println("Limit Heap [" + limit + "]");
    long difference = limit - used;
    int sizeToAllocate = (int) ((int) difference * 0.50);
    System.out.println("Allocating [" + sizeToAllocate + "] Heap [" + getHeapSize() + "] Max [" + getMaxHeapSize()
        + "]");

    byte[] bufferToFillHeap = new byte[sizeToAllocate];
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // This call will take several seconds to execute.
          client.query("test", blurQueryRow);
          fail.set(true);
        } catch (BlurException e) {
          System.out.println("-------------------");
          System.out.println("-------------------");
          System.out.println("-------------------");
          e.printStackTrace();
          System.out.println("-------------------");
          System.out.println("-------------------");
          System.out.println("-------------------");
          error.set(e);
        } catch (TException e) {
          e.printStackTrace();
          fail.set(true);
        }
      }
    }).start();
    Thread.sleep(500);
    List<byte[]> bufferToPutGcWatcherOverLimitList = new ArrayList<byte[]>();
    BlurException blurException = pollForError(error, 120, TimeUnit.SECONDS, bufferToPutGcWatcherOverLimitList, fail,
        (int) (difference / 7));
    if (fail.get()) {
      fail("Unknown error, failing test.");
    }
    System.out.println(bufferToFillHeap.hashCode());
    System.out.println(bufferToPutGcWatcherOverLimitList.hashCode());
    assertEquals(blurException.getErrorType(), ErrorType.BACK_PRESSURE);
    bufferToPutGcWatcherOverLimitList.clear();
    bufferToPutGcWatcherOverLimitList = null;
    bufferToFillHeap = null;
  }

  private BlurException pollForError(AtomicReference<BlurException> error, long period, TimeUnit timeUnit,
      List<byte[]> bufferToPutGcWatcherOverLimitList, AtomicBoolean fail, int sizeToAllocate)
      throws InterruptedException {
    long s = System.nanoTime();
    long totalTime = timeUnit.toNanos(period) + s;
    if (bufferToPutGcWatcherOverLimitList != null) {
      System.out.println("Allocating [" + sizeToAllocate + "] Heap [" + getHeapSize() + "] Max [" + getMaxHeapSize()
          + "]");
      bufferToPutGcWatcherOverLimitList.add(new byte[sizeToAllocate]);
    }
    while (totalTime > System.nanoTime()) {
      if (fail.get()) {
        fail("The query failed.");
      }
      BlurException blurException = error.get();
      if (blurException != null) {
        return blurException;
      }
      Thread.sleep(100);
      if (bufferToPutGcWatcherOverLimitList != null) {
        if (getHeapSize() < (getMaxHeapSize() * 0.8)) {
          System.out.println("Allocating [" + sizeToAllocate + "] Heap [" + getHeapSize() + "] Max ["
              + getMaxHeapSize() + "]");
          bufferToPutGcWatcherOverLimitList.add(new byte[sizeToAllocate]);
        } else {
          System.gc();
          System.gc();
          System.out.println("Already allocated enough Heap [" + getHeapSize() + "] Max [" + getMaxHeapSize() + "]");
        }
      }
    }
    return null;
  }

  private long getHeapSize() {
    return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
  }

  private long getMaxHeapSize() {
    return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
  }

  public void testTestShardFailover() throws BlurException, TException, InterruptedException, IOException,
      KeeperException {

    Iface client = getClient();
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.setUseCacheIfPresent(false);
    Query query = new Query();
    query.setQuery("test.test:value");
    blurQuery.setQuery(query);
    BlurResults results1 = client.query("test", blurQuery);
    assertEquals(numberOfDocs, results1.getTotalResults());
    assertRowResults(results1);

    miniCluster.killShardServer(1);

    // make sure the WAL syncs
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));

    // This should block until shards have failed over
    client.shardServerLayout("test");

    assertEquals(numberOfDocs, client.query("test", blurQuery).getTotalResults());

  }

  public void testTermsList() throws BlurException, TException {
    Iface client = getClient();
    List<String> terms = client.terms("test", "test", "test", null, (short) 10);
    List<String> list = new ArrayList<String>();
    list.add("value");
    assertEquals(list, terms);
  }

  private void assertRowResults(BlurResults results) {
    for (BlurResult result : results.getResults()) {
      assertNull(result.locationId);
      assertNull(result.fetchResult.recordResult);
      assertNull(result.fetchResult.rowResult.row.records);
      assertNotNull(result.fetchResult.rowResult.row.id);
    }
  }

  private void assertRecordResults(BlurResults results) {
    for (BlurResult result : results.getResults()) {
      assertNull(result.locationId);
      assertNotNull(result.fetchResult.recordResult);
      assertNotNull(result.fetchResult.recordResult.rowid);
      assertNotNull(result.fetchResult.recordResult.record.recordId);
      assertNotNull(result.fetchResult.recordResult.record.family);
      assertNull("Not null [" + result.fetchResult.recordResult.record.columns + "]",
          result.fetchResult.recordResult.record.columns);
      assertNull(result.fetchResult.rowResult);
    }
  }

  public void testCreateDisableAndRemoveTable() throws IOException, BlurException, TException {
    Iface client = getClient();
    String tableName = UUID.randomUUID().toString();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(tableName);
    tableDescriptor.setShardCount(5);
    tableDescriptor.setTableUri(miniCluster.getFileSystemUri().toString() + "/blur/" + tableName);

    for (int i = 0; i < 3; i++) {
      client.createTable(tableDescriptor);
      client.disableTable(tableName);
      client.removeTable(tableName, true);
    }

    assertFalse(client.tableList().contains(tableName));

  }
}
