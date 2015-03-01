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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.MiniCluster;
import org.apache.blur.TestType;
import org.apache.blur.analysis.FieldManager;
import org.apache.blur.command.BlurObject;
import org.apache.blur.command.RunSlowForTesting;
import org.apache.blur.command.Shard;
import org.apache.blur.command.UserCurrentUser;
import org.apache.blur.server.TableContext;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.thrift.generated.Facet;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.QueryState;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.SortField;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.thrift.util.BlurThriftHelper;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.GCWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class BlurClusterTestBase {

  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_BlurClusterTest"));
  private static MiniCluster miniCluster;
  private static boolean externalProcesses = false;

  private int numberOfDocs = 1000;
  private String controllerConnectionStr;

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
    miniCluster.startBlurCluster(new File(testDirectory, "cluster").getAbsolutePath(), 2, 3, true, externalProcesses);
  }

  @AfterClass
  public static void shutdownCluster() {
    miniCluster.shutdownBlurCluster();
  }

  @Before
  public void setup() {
    UserContext.setUser(getUser());
  }

  protected abstract User getUser();

  @After
  public void tearDown() throws BlurException, TException {
    Iface client = getClient();
    List<String> tableList = client.tableList();
    for (String table : tableList) {
      client.disableTable(table);
      client.removeTable(table, true);
    }
  }

  protected Iface getClient() {
    if (controllerConnectionStr == null) {
      controllerConnectionStr = miniCluster.getControllerConnectionStr();
    }
    return BlurClient.getClient(controllerConnectionStr);
  }

  @Test
  public void testEnqueue() throws BlurException, TException, InterruptedException, IOException {
    String tableName = "testEnqueue";
    createTable(tableName);
    Blur.Iface client = getClient();

    long s = System.currentTimeMillis();
    int count = 10000;
    for (int i = 0; i < count; i++) {
      String rowId = UUID.randomUUID().toString();
      RecordMutation mutation = BlurThriftHelper.newRecordMutation("test", rowId,
          BlurThriftHelper.newColumn("test", "value"));
      RowMutation rowMutation = BlurThriftHelper.newRowMutation(tableName, rowId, mutation);
      client.enqueueMutate(mutate(rowMutation));
    }
    long e = System.currentTimeMillis();
    double seconds = (e - s) / 1000.0;
    double rate = count / seconds;
    System.out.println("Load row in queue at " + rate + "/s");

    for (int i = 0; i < 60; i++) {
      TableStats stats = client.tableStats(tableName);
      long rowCount = stats.getRowCount();
      if (rowCount == count) {
        return;
      }
      Thread.sleep(1000);
    }
    fail("Test failed to load all rows.");
  }

  protected abstract RowMutation mutate(RowMutation rowMutation);

  @Test
  public void testBlurQueryWithRowId() throws BlurException, TException, InterruptedException, IOException {
    String tableName = "testBlurQueryWithRowId";
    createTable(tableName);
    loadTable(tableName);
    Blur.Iface client = getClient();
    BlurQuery blurQuery = new BlurQuery();
    Query query = new Query();
    query.setQuery("*");
    blurQuery.setQuery(query);
    BlurResults results1 = client.query(tableName, blurQuery);
    assertEquals(numberOfDocs, results1.getTotalResults());
    String id1 = results1.getResults().iterator().next().getFetchResult().getRowResult().getRow().getId();

    blurQuery.setRowId(id1);

    query.setRowQuery(false);
    BlurResults results2 = client.query(tableName, blurQuery);
    assertEquals(1, results2.getTotalResults());
    String id2 = results2.getResults().iterator().next().getFetchResult().getRecordResult().getRowid();

    assertEquals(id1, id2);
    System.out.println("Finished!");
  }

  @Test
  public void testAdminCalls() throws BlurException, TException, IOException, InterruptedException {
    String tableName = "testAdminCalls";
    createTable(tableName);
    loadTable(tableName);
    Blur.Iface client = getClient();
    List<String> shardClusterList = client.shardClusterList();
    assertEquals(1, shardClusterList.size());
    assertEquals(BlurConstants.DEFAULT, shardClusterList.get(0));

    Map<String, String> shardServerLayout = client.shardServerLayout(tableName);
    assertEquals(5, shardServerLayout.size());

    Map<String, Map<String, ShardState>> shardServerLayoutState = client.shardServerLayoutState(tableName);
    assertEquals(5, shardServerLayoutState.size());

    List<String> shardServerList = client.shardServerList(BlurConstants.DEFAULT);
    assertEquals(3, shardServerList.size());
  }

  @Test
  public void testForEmptySchema() throws BlurException, TException, IOException, InterruptedException {
    String tableName = "testForEmptySchema";
    createTable(tableName, false);
    Blur.Iface client = getClient();
    Schema schema = client.schema(tableName);
    Map<String, Map<String, ColumnDefinition>> families = schema.getFamilies();
    assertTrue(families.isEmpty());

    TableContext tableContext = TableContext.create(client.describe(tableName));
    FieldManager fieldManager = tableContext.getFieldManager();

    assertTrue(fieldManager
        .addColumnDefinition("test-family", "test-column", null, false, "string", false, false, null));

    TableContext.clear();
    Schema newschema = client.schema(tableName);
    Map<String, Map<String, ColumnDefinition>> newfamilies = newschema.getFamilies();
    assertTrue(!newfamilies.isEmpty());
    int newsize = newfamilies.size();
    assertEquals(1, newsize);
  }

  @Test
  public void testCreateTableWithCustomType() throws IOException, BlurException, TException {
    Blur.Iface client = getClient();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test_type");
    tableDescriptor.setShardCount(1);
    tableDescriptor.setTableUri(miniCluster.getFileSystemUri().toString() + "/blur/test_type");
    tableDescriptor.putToTableProperties("blur.fieldtype.customtype1", TestType.class.getName());
    setupTableProperties(tableDescriptor);
    client.createTable(tableDescriptor);
    postTableCreate(tableDescriptor, client);
    List<String> tableList = client.tableList();
    assertTrue(tableList.contains("test_type"));

    client.disableTable("test_type");

    client.enableTable("test_type");

    TableDescriptor describe = client.describe("test_type");
    Map<String, String> tableProperties = describe.getTableProperties();
    assertEquals(TestType.class.getName(), tableProperties.get("blur.fieldtype.customtype1"));
  }

  public void createTable(String tableName) throws BlurException, TException, IOException {
    createTable(tableName, true);
  }

  public void createTable(String tableName, boolean applyAcl) throws BlurException, TException, IOException {
    Blur.Iface client = getClient();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(tableName);
    tableDescriptor.setShardCount(5);
    tableDescriptor.setTableUri(miniCluster.getFileSystemUri().toString() + "/blur/" + tableName);
    setupTableProperties(tableDescriptor);
    client.createTable(tableDescriptor);
    if (applyAcl) {
      postTableCreate(tableDescriptor, client);
    }
    List<String> tableList = client.tableList();
    assertTrue(tableList.contains(tableName));
  }

  protected abstract void postTableCreate(TableDescriptor tableDescriptor, Iface client);

  protected abstract void setupTableProperties(TableDescriptor tableDescriptor);

  public void loadTable(String tableName) throws BlurException, TException, InterruptedException {
    loadTable(tableName, 1);
  }

  public void loadTable(String tableName, int pass) throws BlurException, TException, InterruptedException {
    Iface client = getClient();
    int maxFacetValue = 100;
    List<RowMutation> mutations = new ArrayList<RowMutation>();
    Random random = new Random(1);
    for (int i = 0; i < numberOfDocs; i++) {
      String rowId = UUID.randomUUID().toString();
      RecordMutation mutation = BlurThriftHelper.newRecordMutation("test", rowId,
          BlurThriftHelper.newColumn("test", "value"),
          BlurThriftHelper.newColumn("facet", Integer.toString(random.nextInt(maxFacetValue))),
          BlurThriftHelper.newColumn("facetFixed", "test"));
      RowMutation rowMutation = BlurThriftHelper.newRowMutation(tableName, rowId, mutation);
      mutations.add(rowMutation);
    }
    ColumnDefinition columnDefinition = new ColumnDefinition();
    columnDefinition.setFamily("test");
    columnDefinition.setColumnName("facet");
    columnDefinition.setFieldLessIndexed(true);
    columnDefinition.setFieldType("string");
    columnDefinition.setSortable(true);
    columnDefinition.setMultiValueField(false);
    columnDefinition.setProperties(new HashMap<String, String>());
    client.addColumnDefinition(tableName, columnDefinition);
    boolean bulkMutate = false;
    long s = System.nanoTime();
    if (bulkMutate) {
      String bulkId = UUID.randomUUID().toString();
      client.bulkMutateStart(bulkId);
      client.bulkMutateAddMultiple(bulkId, mutate(mutations));
      client.bulkMutateFinish(bulkId, true, true);
    } else {
      client.mutateBatch(mutate(mutations));
    }
    long e = System.nanoTime();
    System.out.println("mutateBatch took [" + (e - s) / 1000000.0 + "]");
    BlurQuery blurQueryRow = new BlurQuery();
    Query queryRow = new Query();
    queryRow.setQuery("test.test:value");
    blurQueryRow.setQuery(queryRow);
    blurQueryRow.setUseCacheIfPresent(false);
    blurQueryRow.setCacheResult(false);
    BlurResults resultsRow = client.query(tableName, blurQueryRow);
    assertRowResults(resultsRow);
    assertEquals(numberOfDocs * pass, resultsRow.getTotalResults());

    BlurQuery blurQueryRecord = new BlurQuery();
    Query queryRecord = new Query();
    queryRecord.rowQuery = false;
    queryRecord.setQuery("test.test:value");
    blurQueryRecord.setQuery(queryRecord);
    BlurResults resultsRecord = client.query(tableName, blurQueryRecord);
    assertRecordResults(resultsRecord);
    assertEquals(numberOfDocs * pass, resultsRecord.getTotalResults());

    Schema schema = client.schema(tableName);
    assertFalse(schema.getFamilies().isEmpty());
  }

  protected List<RowMutation> mutate(List<RowMutation> mutations) {
    List<RowMutation> rowMutations = new ArrayList<RowMutation>();
    for (int i = 0; i < mutations.size(); i++) {
      rowMutations.add(mutate(mutations.get(i)));
    }
    return rowMutations;
  }

  @Test
  public void testQueryWithSelector() throws BlurException, TException, IOException, InterruptedException {
    final String tableName = "testQueryWithSelector";
    createTable(tableName);
    loadTable(tableName);
    Iface client = getClient();
    BlurQuery blurQueryRow = new BlurQuery();
    Query queryRow = new Query();
    queryRow.setQuery("test.test:value");
    blurQueryRow.setQuery(queryRow);
    blurQueryRow.setUseCacheIfPresent(false);
    blurQueryRow.setCacheResult(false);
    blurQueryRow.setSelector(new Selector());

    BlurResults resultsRow = client.query(tableName, blurQueryRow);
    // assertRowResults(resultsRow);
    assertEquals(numberOfDocs, resultsRow.getTotalResults());

    for (BlurResult blurResult : resultsRow.getResults()) {
      System.out.println(blurResult);
    }

  }

  @Test
  public void testSortedQueryWithSelector() throws BlurException, TException, IOException, InterruptedException {
    final String tableName = "testSortedQueryWithSelector";
    createTable(tableName);
    loadTable(tableName);

    Iface client = getClient();

    BlurQuery blurQueryRow = new BlurQuery();
    Query queryRow = new Query();
    queryRow.setQuery("test.test:value");
    queryRow.setRowQuery(false);
    blurQueryRow.setQuery(queryRow);
    blurQueryRow.addToSortFields(new SortField("test", "facet", false));

    blurQueryRow.setUseCacheIfPresent(false);
    blurQueryRow.setCacheResult(false);
    Selector selector = new Selector();
    selector.setRecordOnly(true);
    blurQueryRow.setSelector(selector);

    BlurResults resultsRow = client.query(tableName, blurQueryRow);
    long totalResults = resultsRow.getTotalResults();

    assertEquals(numberOfDocs, resultsRow.getTotalResults());

    String lastValue = null;
    long totalFetched = 0;
    do {
      for (BlurResult blurResult : resultsRow.getResults()) {
        FetchResult fetchResult = blurResult.getFetchResult();
        Record record = fetchResult.getRecordResult().getRecord();
        if (lastValue == null) {
          lastValue = getColumnValue(record, "facet");
        } else {
          String currentValue = getColumnValue(record, "facet");
          if (currentValue.compareTo(lastValue) < 0) {
            fail("Current Value of [" + currentValue + "] can not be less than lastValue of [" + lastValue + "]");
          }
          lastValue = currentValue;
        }
        totalFetched++;
      }
      int size = resultsRow.getResults().size();
      totalResults -= size;
      if (totalResults > 0) {
        blurQueryRow.setStart(blurQueryRow.getStart() + size);
        resultsRow = client.query(tableName, blurQueryRow);
      }
    } while (totalResults > 0);
    assertEquals(numberOfDocs, totalFetched);
  }

  private String getColumnValue(Record record, String columnName) {
    for (Column col : record.getColumns()) {
      if (col.getName().equals(columnName)) {
        return col.getValue();
      }
    }
    return null;
  }

  // @Test
  public void testQueryWithSelectorForDeepPagingPerformance() throws BlurException, TException, IOException,
      InterruptedException {
    final String tableName = "testQueryWithSelectorForDeepPagingPerformance";
    createTable(tableName);
    int passes = 10;
    for (int i = 1; i <= passes; i++) {
      loadTable(tableName, i);
    }
    Iface client = getClient();
    BlurQuery blurQueryRow = new BlurQuery();
    Query queryRow = new Query();
    queryRow.setQuery("test.test:value");
    blurQueryRow.setQuery(queryRow);
    blurQueryRow.setUseCacheIfPresent(false);
    blurQueryRow.setCacheResult(false);
    blurQueryRow.setSelector(new Selector());

    long start = System.nanoTime();
    int position = 0;
    do {
      blurQueryRow.setStart(position);
      long s = System.nanoTime();
      BlurResults resultsRow = client.query(tableName, blurQueryRow);
      long e = System.nanoTime();
      System.out.println("RUNNING QUERY.... starting at [" + position + "] took [" + (e - s) / 1000000.0 + " ms]");
      // assertRowResults(resultsRow);
      assertEquals(numberOfDocs * passes, resultsRow.getTotalResults());

      for (BlurResult blurResult : resultsRow.getResults()) {
        System.out.println(blurResult);
        position++;
      }
    } while (position < numberOfDocs * passes);
    long end = System.nanoTime();
    System.out.println((end - start) / 1000000.0);
  }

  @Test
  public void testQueryWithFacets() throws BlurException, TException, IOException, InterruptedException {
    final String tableName = "testQueryWithFacets";
    createTable(tableName);
    loadTable(tableName);
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

    BlurResults resultsRow = client.query(tableName, blurQueryRow);
    assertEquals(numberOfDocs, resultsRow.getTotalResults());
    System.out.println(resultsRow.getFacetCounts());

    System.out.println();

  }

  @Test
  public void testBatchFetch() throws BlurException, TException, InterruptedException, IOException {
    String tableName = "testBatchFetch";
    long t1 = System.nanoTime();
    createTable(tableName);
    long t2 = System.nanoTime();
    loadTable(tableName);
    long t3 = System.nanoTime();
    final Iface client = getClient();
    List<String> terms = client.terms(tableName, null, "rowid", "", (short) 100);

    List<Selector> selectors = new ArrayList<Selector>();
    for (String s : terms) {
      Selector selector = new Selector();
      selector.setRowId(s);
      selectors.add(selector);
    }

    List<FetchResult> fetchRowBatch = client.fetchRowBatch(tableName, selectors);
    assertEquals(100, fetchRowBatch.size());

    int i = 0;
    for (FetchResult fetchResult : fetchRowBatch) {
      assertEquals(terms.get(i), fetchResult.getRowResult().getRow().getId());
      i++;
    }

    System.out.println("Create table took [" + (t2 - t1) / 1000000.0 + "]");
    System.out.println("Load table took [" + (t3 - t2) / 1000000.0 + "]");
  }

  @Test
  public void testQueryStatus() throws BlurException, TException, InterruptedException, IOException {
    final String tableName = "testQueryStatus";
    createTable(tableName);
    loadTable(tableName);
    final Iface client = getClient();
    final BlurQuery blurQueryRow = new BlurQuery();
    Query queryRow = new Query();
    queryRow.setQuery("test.test:value");
    blurQueryRow.setQuery(queryRow);
    blurQueryRow.setUseCacheIfPresent(false);
    blurQueryRow.setCacheResult(false);
    String uuid = "5678";
    blurQueryRow.setUuid(uuid);
    final User user = new User("testuser", getUserAttributes());
    try {
      setDebugRunSlow(tableName, true);
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            UserContext.setUser(user);
            // This call will take several seconds to execute.
            client.query(tableName, blurQueryRow);
          } catch (BlurException e) {
            // e.printStackTrace();
          } catch (TException e) {
            e.printStackTrace();
          }
        }
      }).start();
      Thread.sleep(500);
      BlurQueryStatus queryStatusById = client.queryStatusById(tableName, uuid);
      assertEquals(user.getUsername(), queryStatusById.getUser().getUsername());
      assertEquals(queryStatusById.getState(), QueryState.RUNNING);
      client.cancelQuery(tableName, uuid);
    } finally {
      setDebugRunSlow(tableName, false);
    }
  }

  protected abstract Map<String, String> getUserAttributes();

  @Test
  public void testQueryCancel() throws BlurException, TException, InterruptedException, IOException {
    final String tableName = "testQueryCancel";
    createTable(tableName);
    loadTable(tableName);
    final Iface client = getClient();
    try {
      // This will make each collect in the collectors pause 250 ms per collect
      // call
      setDebugRunSlow(tableName, true);
      final BlurQuery blurQueryRow = new BlurQuery();
      Query queryRow = new Query();
      queryRow.setQuery("test.test:value");
      blurQueryRow.setQuery(queryRow);
      blurQueryRow.setUseCacheIfPresent(false);
      blurQueryRow.setCacheResult(false);
      blurQueryRow.setUuid("1234");

      final AtomicReference<BlurException> error = new AtomicReference<BlurException>();
      final AtomicBoolean fail = new AtomicBoolean();
      final User user = UserContext.getUser();
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            UserContext.setUser(user);
            // This call will take several seconds to execute.
            client.query(tableName, blurQueryRow);
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
      client.cancelQuery(tableName, blurQueryRow.getUuid());
      BlurException blurException = pollForError(error, 10, TimeUnit.SECONDS, null, fail, -1);
      if (fail.get()) {
        fail("Unknown error, failing test.");
      }
      assertEquals(blurException.getErrorType(), ErrorType.QUERY_CANCEL);
    } finally {
      setDebugRunSlow(tableName, false);
    }
    // Tests that the exitable reader was reset.
    client.terms(tableName, "test", "facet", null, (short) 100);
  }

  // @Test
  public void testBackPressureViaQuery() throws BlurException, TException, InterruptedException, IOException {
    // This will make each collect in the collectors pause 250 ms per collect
    // call
    String tableName = "testAdminCalls";
    createTable(tableName);
    loadTable(tableName);
    try {
      setDebugRunSlow(tableName, true);
      runBackPressureViaQuery(tableName);
      Thread.sleep(1000);
      System.gc();
      System.gc();
      Thread.sleep(1000);
    } finally {
      setDebugRunSlow(tableName, false);
    }
  }

  private void setDebugRunSlow(String table, boolean flag) throws IOException {
    RunSlowForTesting runSlowForTesting = new RunSlowForTesting();
    runSlowForTesting.setRunSlow(flag);
    runSlowForTesting.setTable(table);
    runSlowForTesting.run(getClient());
  }

  private void runBackPressureViaQuery(final String tableName) throws InterruptedException {
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
    final User user = UserContext.getUser();
    new Thread(new Runnable() {
      @Override
      public void run() {
        UserContext.setUser(user);
        try {
          // This call will take several seconds to execute.
          client.query(tableName, blurQueryRow);
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

  @Test
  public void testTestShardFailover() throws BlurException, TException, InterruptedException, IOException,
      KeeperException {
    final String tableName = "testTestShardFailover";
    createTable(tableName);
    loadTable(tableName);
    Iface client = getClient();
    BlurQuery blurQuery = new BlurQuery();
    Query query = new Query();
    query.setQuery("test.test:value");
    blurQuery.setQuery(query);
    BlurResults results1 = client.query(tableName, blurQuery);
    assertEquals(numberOfDocs, results1.getTotalResults());
    assertRowResults(results1);

    miniCluster.killShardServer(1);

    // make sure the WAL syncs
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));

    // This should block until shards have failed over
    client.shardServerLayout(tableName);

    assertEquals("We should have lost a node.", 2, client.shardServerList(BlurConstants.DEFAULT).size());
    assertEquals(numberOfDocs, client.query(tableName, blurQuery).getTotalResults());

    miniCluster.startShards(1, true, externalProcesses);
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));

    assertEquals("We should have the cluster back where we started.", 3, client.shardServerList(BlurConstants.DEFAULT)
        .size());
  }

  @Test
  public void testTruncateRaceCondition() throws BlurException, TException, IOException, InterruptedException {
    String tableName = "testTruncateRaceCondition";
    createTable(tableName);
    loadTable(tableName);
    List<Connection> connections = BlurClientManager.getConnections(miniCluster.getControllerConnectionStr());
    Iface client1 = BlurClient.getClient(connections.get(0));
    Iface client2 = BlurClient.getClient(connections.get(1));
    TableDescriptor describe = client1.describe(tableName);
    setupTableProperties(describe);
    client1.disableTable(tableName);
    client1.removeTable(tableName, true);
    client1.createTable(describe);
    postTableCreate(describe, client1);

    String rowId = UUID.randomUUID().toString();
    RecordMutation mutation = BlurThriftHelper.newRecordMutation("test", rowId,
        BlurThriftHelper.newColumn("test", "value"), BlurThriftHelper.newColumn("facetFixed", "test"));
    RowMutation rowMutation = BlurThriftHelper.newRowMutation(tableName, rowId, mutation);
    client2.mutate(mutate(rowMutation));
  }

  @Test
  public void testTableRemovalWithFieldDefsV1() throws BlurException, TException, IOException, InterruptedException {
    String tableName = "testTableRemovalWithFieldDefsV1";
    createTable(tableName);
    loadTable(tableName);

    String family = "testTableRemovalWithFieldDefs-fam";

    List<Connection> connections = BlurClientManager.getConnections(miniCluster.getControllerConnectionStr());
    Iface client1 = BlurClient.getClient(connections.get(0));
    ColumnDefinition columnDefinition = new ColumnDefinition();
    columnDefinition.setColumnName("col");
    columnDefinition.setFamily(family);
    columnDefinition.setFieldLessIndexed(false);
    columnDefinition.setFieldType("string");
    columnDefinition.setSortable(false);
    columnDefinition.setSubColumnName(null);
    assertTrue(client1.addColumnDefinition(tableName, columnDefinition));

    client1.disableTable(tableName);
    client1.removeTable(tableName, true);

    createTable(tableName);

    Iface client2 = BlurClient.getClient(connections.get(1));
    assertFamilyIsNotPresent(tableName, client2, family);

    List<String> shardClusterList = client2.shardClusterList();

    for (String cluster : shardClusterList) {
      List<String> shardServerList = client2.shardServerList(cluster);
      for (String shardServer : shardServerList) {
        Iface client = BlurClient.getClient(shardServer);
        assertFamilyIsNotPresent(tableName, client, family);
      }
    }

  }

  @Test
  public void testTableRemovalWithFieldDefsV2() throws BlurException, TException, IOException, InterruptedException {
    String tableName = "testTableRemovalWithFieldDefsV2";
    createTable(tableName);
    loadTable(tableName);

    Iface client = getClient();
    Schema expectedSchema = client.schema(tableName);
    Set<String> expectedFiles = new TreeSet<String>();
    TableDescriptor describe = client.describe(tableName);

    {
      Path path = new Path(describe.getTableUri());
      FileSystem fileSystem = path.getFileSystem(new Configuration());
      FileStatus[] listStatus = fileSystem.listStatus(new Path(path, "types"));
      for (FileStatus fileStatus : listStatus) {
        System.out.println("Expected " + fileStatus.getPath());
        expectedFiles.add(fileStatus.getPath().toString());
      }
    }

    client.disableTable(tableName);
    client.removeTable(tableName, true);

    createTable(tableName);
    loadTable(tableName);
    Schema actualSchema = client.schema(tableName);

    assertEquals(expectedSchema.getTable(), actualSchema.getTable());
    Map<String, Map<String, ColumnDefinition>> expectedFamilies = expectedSchema.getFamilies();
    Map<String, Map<String, ColumnDefinition>> actualFamilies = actualSchema.getFamilies();
    assertEquals(new TreeSet<String>(expectedFamilies.keySet()), new TreeSet<String>(actualFamilies.keySet()));

    for (String family : expectedFamilies.keySet()) {
      Map<String, ColumnDefinition> expectedColDefMap = new TreeMap<String, ColumnDefinition>(
          expectedFamilies.get(family));
      Map<String, ColumnDefinition> actualColDefMap = new TreeMap<String, ColumnDefinition>(actualFamilies.get(family));
      assertEquals(expectedColDefMap, actualColDefMap);
    }

    System.out.println(expectedSchema);

    Set<String> actualFiles = new TreeSet<String>();
    {
      Path path = new Path(describe.getTableUri());
      FileSystem fileSystem = path.getFileSystem(new Configuration());
      FileStatus[] listStatus = fileSystem.listStatus(new Path(path, "types"));
      for (FileStatus fileStatus : listStatus) {
        System.out.println("Actual " + fileStatus.getPath());
        actualFiles.add(fileStatus.getPath().toString());
      }
    }
    assertEquals(expectedFiles, actualFiles);
  }

  private void assertFamilyIsNotPresent(String tableName, Iface client, String family) throws BlurException, TException {
    Schema schema = client.schema(tableName);
    Map<String, Map<String, ColumnDefinition>> families = schema.getFamilies();
    assertNull(families.get(family));
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

  @Test
  public void testCreateDisableAndRemoveTable() throws IOException, BlurException, TException {
    Iface client = getClient();
    String tableName = UUID.randomUUID().toString();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(tableName);
    tableDescriptor.setShardCount(5);
    tableDescriptor.setTableUri(miniCluster.getFileSystemUri().toString() + "/blur/" + tableName);
    setupTableProperties(tableDescriptor);
    for (int i = 0; i < 3; i++) {
      client.createTable(tableDescriptor);
      postTableCreate(tableDescriptor, client);
      client.disableTable(tableName);
      client.removeTable(tableName, true);
    }

    assertFalse(client.tableList().contains(tableName));

  }

  @Test
  public void testBulkMutate() throws BlurException, TException, IOException {
    String tableName = "testBulkMutate";
    createTable(tableName);

    String bulkId = UUID.randomUUID().toString();
    Iface client = getClient();
    client.bulkMutateStart(bulkId);
    int batchSize = 11;
    int total = 10000;
    int maxFacetValue = 100;
    List<RowMutation> mutations = new ArrayList<RowMutation>();
    Random random = new Random(1);
    for (int i = 0; i < total; i++) {
      String rowId = UUID.randomUUID().toString();
      RecordMutation mutation = BlurThriftHelper.newRecordMutation("test", rowId,
          BlurThriftHelper.newColumn("test", "value"),
          BlurThriftHelper.newColumn("facet", Integer.toString(random.nextInt(maxFacetValue))),
          BlurThriftHelper.newColumn("facetFixed", "test"));
      RowMutation rowMutation = BlurThriftHelper.newRowMutation(tableName, rowId, mutation);
      mutations.add(rowMutation);
      if (mutations.size() >= batchSize) {
        client.bulkMutateAddMultiple(bulkId, mutations);
        mutations.clear();
      }
    }
    if (mutations.size() > 0) {
      client.bulkMutateAddMultiple(bulkId, mutations);
      mutations.clear();
    }
    client.bulkMutateFinish(bulkId, true, true);

    TableStats tableStats = client.tableStats(tableName);
    assertEquals(total, tableStats.getRecordCount());
  }

  @Test
  public void testCommandUserProgation() throws IOException, BlurException, TException {
    String table = "testCommandUserProgation";
    createTable(table);
    Iface client = getClient();
    Map<String, String> attributes = new HashMap<String, String>();
    attributes.put("att1", "value");
    User user = new User(table, attributes);
    UserContext.setUser(user);
    UserCurrentUser userCurrentUser = new UserCurrentUser();
    userCurrentUser.setTable(table);
    Map<Shard, BlurObject> currentUser = userCurrentUser.run(client);

    BlurObject blurObject = new BlurObject();
    blurObject.put("username", table);
    BlurObject attributesBlurObject = new BlurObject();
    attributesBlurObject.put("att1", "value");
    blurObject.put("attributes", attributesBlurObject);

    for (Entry<Shard, BlurObject> e : currentUser.entrySet()) {
      assertEquals(blurObject, e.getValue());
    }
  }

}
