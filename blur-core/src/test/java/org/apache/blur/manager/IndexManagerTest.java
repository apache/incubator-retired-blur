package org.apache.blur.manager;

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

import static org.apache.blur.thrift.generated.RecordMutationType.APPEND_COLUMN_VALUES;
import static org.apache.blur.thrift.generated.RecordMutationType.DELETE_ENTIRE_RECORD;
import static org.apache.blur.thrift.generated.RecordMutationType.REPLACE_COLUMNS;
import static org.apache.blur.thrift.generated.RecordMutationType.REPLACE_ENTIRE_RECORD;
import static org.apache.blur.thrift.generated.RowMutationType.DELETE_ROW;
import static org.apache.blur.thrift.generated.RowMutationType.UPDATE_ROW;
import static org.apache.blur.thrift.util.BlurThriftHelper.match;
import static org.apache.blur.thrift.util.BlurThriftHelper.newColumn;
import static org.apache.blur.thrift.util.BlurThriftHelper.newRecord;
import static org.apache.blur.thrift.util.BlurThriftHelper.newRecordMutation;
import static org.apache.blur.thrift.util.BlurThriftHelper.newRow;
import static org.apache.blur.thrift.util.BlurThriftHelper.newRowMutation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.lucene.search.DeepPagingCache;
import org.apache.blur.manager.clusterstatus.ClusterStatus;
import org.apache.blur.manager.indexserver.LocalIndexServer;
import org.apache.blur.manager.results.BlurResultIterable;
import org.apache.blur.memory.MemoryAllocationWatcher;
import org.apache.blur.memory.Watcher;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Facet;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.HighlightOptions;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.trace.BaseTraceStorage;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.TraceCollector;
import org.apache.blur.trace.TraceStorage;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurIterator;
import org.apache.blur.utils.ShardUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexManagerTest {

  private static final File TMPDIR = new File("./target/tmp");

  private static final String SHARD_NAME = ShardUtil.getShardName(BlurConstants.SHARD_PREFIX, 0);
  private static final String TABLE = "table";
  private static final String FAMILY = "test-family";
  private static final String FAMILY2 = "test-family2";
  private static final MemoryAllocationWatcher NOTHING = new MemoryAllocationWatcher() {
    @Override
    public <T, E extends Exception> T run(Watcher<T, E> w) throws E {
      return w.run();
    }
  };
  private LocalIndexServer server;
  private IndexManager indexManager;
  private File base;

  @Before
  public void setUp() throws BlurException, IOException, InterruptedException {
    TableContext.clear();
    base = new File(TMPDIR, "blur-index-manager-test");
    rm(base);

    File file = new File(base, TABLE);
    file.mkdirs();

    IndexManagerTestReadInterceptor.interceptor = null;

    final TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(TABLE);
    tableDescriptor.setTableUri(file.toURI().toString());
    tableDescriptor.putToTableProperties("blur.shard.time.between.refreshs", Long.toString(100));
    tableDescriptor.setShardCount(1);
    tableDescriptor.putToTableProperties(BlurConstants.BLUR_SHARD_READ_INTERCEPTOR,
        IndexManagerTestReadInterceptor.class.getName());
    server = new LocalIndexServer(tableDescriptor, true);

    BlurFilterCache filterCache = new DefaultBlurFilterCache(new BlurConfiguration());
    long statusCleanupTimerDelay = 1000;
    indexManager = new IndexManager(server, getClusterStatus(tableDescriptor), filterCache, 10000000, 100, 1, 1,
        statusCleanupTimerDelay, 0, new DeepPagingCache(), NOTHING);
    setupData();
  }

  private ClusterStatus getClusterStatus(final TableDescriptor tableDescriptor) {
    return new ClusterStatus() {

      @Override
      public void removeTable(String cluster, String table, boolean deleteIndexFiles) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public boolean isReadOnly(boolean useCache, String cluster, String table) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public boolean isOpen() {
        throw new RuntimeException("Not impl");
      }

      @Override
      public boolean isInSafeMode(boolean useCache, String cluster) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public boolean isEnabled(boolean useCache, String cluster, String table) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public List<String> getTableList(boolean useCache, String cluster) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public TableDescriptor getTableDescriptor(boolean useCache, String cluster, String table) {
        return tableDescriptor;
      }

      @Override
      public List<String> getShardServerList(String cluster) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public List<String> getOnlineShardServers(boolean useCache, String cluster) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public List<String> getOnlineControllerList() {
        throw new RuntimeException("Not impl");
      }

      @Override
      public List<String> getControllerServerList() {
        throw new RuntimeException("Not impl");
      }

      @Override
      public List<String> getClusterList(boolean useCache) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public String getCluster(boolean useCache, String table) {
        return BlurConstants.BLUR_CLUSTER;
      }

      @Override
      public boolean exists(boolean useCache, String cluster, String table) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public void enableTable(String cluster, String table) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public void disableTable(String cluster, String table) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public void createTable(TableDescriptor tableDescriptor) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public void registerActionOnTableStateChange(Action action) {
        throw new RuntimeException("Not impl");
      }
    };
  }

  @After
  public void teardown() {
    indexManager.close();
    indexManager = null;
    server = null;
  }

  private void rm(File file) {
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }

  private void setupData() throws BlurException, IOException {
    RowMutation mutation1 = newRowMutation(
        TABLE,
        "row-1",
        newRecordMutation(FAMILY, "record-1", newColumn("testcol1", "value1"), newColumn("testcol2", "value2"),
            newColumn("testcol3", "value3")));
    RowMutation mutation2 = newRowMutation(
        TABLE,
        "row-2",
        newRecordMutation(FAMILY, "record-2", newColumn("testcol1", "value4"), newColumn("testcol2", "value5"),
            newColumn("testcol3", "value6")),
        newRecordMutation(FAMILY, "record-2B", newColumn("testcol2", "value234123"),
            newColumn("testcol3", "value234123")));
    RowMutation mutation3 = newRowMutation(
        TABLE,
        "row-3",
        newRecordMutation(FAMILY, "record-3", newColumn("testcol1", "value7"), newColumn("testcol2", "value8"),
            newColumn("testcol3", "value9")));
    RowMutation mutation4 = newRowMutation(
        TABLE,
        "row-4",
        newRecordMutation(FAMILY, "record-4", newColumn("testcol1", "value1"), newColumn("testcol2", "value5"),
            newColumn("testcol3", "value9")),
        newRecordMutation(FAMILY, "record-4B", newColumn("testcol2", "value234123"),
            newColumn("testcol3", "value234123")));
    RowMutation mutation5 = newRowMutation(
        TABLE,
        "row-5",
        newRecordMutation(FAMILY, "record-5A", newColumn("testcol1", "value13"), newColumn("testcol2", "value14"),
            newColumn("testcol3", "value15")),
        newRecordMutation(FAMILY, "record-5B", newColumn("testcol1", "value16"), newColumn("testcol2", "value17"),
            newColumn("testcol3", "value18"), newColumn("testcol3", "value19")));
    RowMutation mutation6 = newRowMutation(TABLE, "row-6",
        newRecordMutation(FAMILY, "record-6A", newColumn("testcol12", "value110"), newColumn("testcol13", "value102")),
        newRecordMutation(FAMILY, "record-6B", newColumn("testcol12", "value101"), newColumn("testcol13", "value104")),
        newRecordMutation(FAMILY2, "record-6C", newColumn("testcol18", "value501")));
    RowMutation mutation7 = newRowMutation(TABLE, "row-7",
        newRecordMutation(FAMILY, "record-7A", newColumn("testcol12", "value101"), newColumn("testcol13", "value102")),
        newRecordMutation(FAMILY2, "record-7B", newColumn("testcol18", "value501")));
    indexManager.mutate(mutation1);
    indexManager.mutate(mutation2);
    indexManager.mutate(mutation3);
    indexManager.mutate(mutation4);
    indexManager.mutate(mutation5);
    indexManager.mutate(mutation6);
    indexManager.mutate(mutation7);
  }

  @Test
  public void testMutationReplaceLargeRow() throws Exception {
    final String rowId = "largerow";
    indexManager.mutate(getLargeRow(rowId, RowMutationType.REPLACE_ROW, 10000));
    TraceStorage oldReporter = Trace.getStorage();
    Trace.setStorage(new BaseTraceStorage(new BlurConfiguration()) {

      @Override
      public void close() throws IOException {

      }

      @Override
      public void store(TraceCollector collector) {
        try {
          System.out.println(collector.toJsonObject());
        } catch (JSONException e) {
          e.printStackTrace();
        }
      }
    });

    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < 25; i++) {
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          for (int t = 0; t < 50; t++) {
            // Trace.setupTrace(rowId);
            Selector selector = new Selector().setRowId(rowId);
            FetchResult fetchResult = new FetchResult();
            long s = System.nanoTime();
            try {
              indexManager.fetchRow(TABLE, selector, fetchResult);
            } catch (BlurException e1) {
              e1.printStackTrace();
            }
            long e = System.nanoTime();
            assertNotNull(fetchResult.rowResult.row);
            // Trace.tearDownTrace();
            System.out.println((e - s) / 1000000.0);
          }
        }
      });
      threads.add(thread);
      thread.start();
      // thread.join();
    }

    for (Thread t : threads) {
      t.join();
    }

    Trace.setStorage(oldReporter);

  }

  @Test
  public void testMutationAppendLargeRow() throws Exception {
    final String rowId = "largerowappend";
    int batch = 2;
    int batchSize = 10000;
    for (int i = 0; i < batch; i++) {
      System.out.println("Adding Batch [" + i + "]");
      indexManager.mutate(getLargeRow(rowId, RowMutationType.UPDATE_ROW, batchSize));
    }

    FetchResult fetchResult = new FetchResult();
    Selector selector = new Selector();
    selector.setRowId(rowId);
    indexManager.fetchRow(TABLE, selector, fetchResult);

    FetchRowResult fetchRowResult = fetchResult.getRowResult();
    System.out.println(fetchRowResult.getTotalRecords());
    assertEquals(batch * batchSize, fetchRowResult.getTotalRecords());
  }

  private RowMutation getLargeRow(String rowId, RowMutationType rowMutationType, int count) {
    RowMutation rowMutation = new RowMutation();
    rowMutation.setTable(TABLE);
    rowMutation.setRowId(rowId);
    rowMutation.setRecordMutations(getRecordMutations(count));
    rowMutation.setRowMutationType(rowMutationType);
    return rowMutation;
  }

  private List<RecordMutation> getRecordMutations(int count) {
    List<RecordMutation> mutations = new ArrayList<RecordMutation>();
    for (int i = 0; i < count; i++) {
      mutations.add(getRecordMutation());
    }
    return mutations;
  }

  private RecordMutation getRecordMutation() {
    RecordMutation mutation = new RecordMutation();
    mutation.setRecordMutationType(REPLACE_ENTIRE_RECORD);
    Record record = new Record();
    record.setFamily(FAMILY);
    record.setRecordId(UUID.randomUUID().toString());
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      record.addToColumns(new Column("col" + i, "value" + random.nextLong()));
    }
    mutation.setRecord(record);
    return mutation;
  }

  @Test
  public void testFetchRowByRowIdHighlighting() throws Exception {
    Selector selector = new Selector().setRowId("row-6");
    HighlightOptions highlightOptions = new HighlightOptions();
    Query query = new Query();
    query.setQuery(FAMILY2 + ".testcol13:value105 " + FAMILY + ".testcol12:value101");
    highlightOptions.setQuery(query);
    selector.setHighlightOptions(highlightOptions);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);

    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow("row-6", newRecord(FAMILY, "record-6B", newColumn("testcol12", "<<<value101>>>")));
    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(3, rowResult.getTotalRecords());
  }

  @Test
  public void testFetchRowByRowIdHighlightingWithFullText() throws Exception {
    Selector selector = new Selector().setRowId("row-6");
    HighlightOptions highlightOptions = new HighlightOptions();
    Query query = new Query();
    query.setQuery("cool value101");
    highlightOptions.setQuery(query);
    selector.setHighlightOptions(highlightOptions);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);

    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow("row-6", newRecord(FAMILY, "record-6B", newColumn("testcol12", "<<<value101>>>")));
    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(3, rowResult.getTotalRecords());
  }

  @Test
  public void testFetchRowByRowIdHighlightingWithFullTextWildCard() throws Exception {
    Selector selector = new Selector().setRowId("row-6");
    HighlightOptions highlightOptions = new HighlightOptions();
    Query query = new Query();
    query.setQuery("cool ?alue101");
    highlightOptions.setQuery(query);
    selector.setHighlightOptions(highlightOptions);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);

    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow("row-6", newRecord(FAMILY, "record-6B", newColumn("testcol12", "<<<value101>>>")));

    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(3, rowResult.getTotalRecords());
  }

  @Test
  public void testFetchRecordByLocationIdHighlighting() throws Exception {
    Selector selector = new Selector().setLocationId(SHARD_NAME + "/0").setRecordOnly(true);
    HighlightOptions highlightOptions = new HighlightOptions();
    Query query = new Query();
    query.setQuery(FAMILY + ".testcol1:value1");
    query.setRowQuery(false);
    highlightOptions.setQuery(query);
    selector.setHighlightOptions(highlightOptions);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNull(fetchResult.rowResult);
    assertNotNull(fetchResult.recordResult.record);

    assertEquals("row-1", fetchResult.recordResult.rowid);
    assertEquals("record-1", fetchResult.recordResult.record.recordId);
    assertEquals(FAMILY, fetchResult.recordResult.record.family);

    Record record = newRecord(FAMILY, "record-1", newColumn("testcol1", "<<<value1>>>"));
    assertEquals(record, fetchResult.recordResult.record);
  }

  @Test
  public void testQueryWithJoinAll() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.query = new Query();
    blurQuery.query.query = "+<+test-family.testcol12:value101 +test-family.testcol13:value102> +<test-family2.testcol18:value501>";

    blurQuery.query.rowQuery = true;
    blurQuery.query.scoreType = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = "1";

    BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
    assertEquals(iterable.getTotalResults(), 1);
    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult result = iterator.next();
      Selector selector = new Selector().setLocationId(result.getLocationId());
      FetchResult fetchResult = new FetchResult();
      indexManager.fetchRow(TABLE, selector, fetchResult);
      assertNotNull(fetchResult.rowResult);
      assertNull(fetchResult.recordResult);
    }
  }

  @Test
  public void testQueryWithJoin() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.query = new Query();
    blurQuery.query.query = "+<+test-family.testcol12:value101 +test-family.testcol13:value102> +<test-family2.testcol18:value501>";
    blurQuery.query.rowQuery = true;
    blurQuery.query.scoreType = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = "1";

    BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
    assertEquals(iterable.getTotalResults(), 1);
    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult result = iterator.next();
      Selector selector = new Selector().setLocationId(result.getLocationId());
      FetchResult fetchResult = new FetchResult();
      indexManager.fetchRow(TABLE, selector, fetchResult);
      assertNotNull(fetchResult.rowResult);
      assertNull(fetchResult.recordResult);
    }
  }

  @Test
  public void testQueryWithJoinForcingSuperQuery() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.query = new Query();
    blurQuery.query.query = "+<test-family.testcol1:value1> +<test-family.testcol3:value234123>";
    blurQuery.query.rowQuery = true;
    blurQuery.query.scoreType = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = "1";

    BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
    assertEquals(iterable.getTotalResults(), 1);
    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult result = iterator.next();
      Selector selector = new Selector().setLocationId(result.getLocationId());
      FetchResult fetchResult = new FetchResult();
      indexManager.fetchRow(TABLE, selector, fetchResult);
      assertNotNull(fetchResult.rowResult);
      assertNull(fetchResult.recordResult);
    }
  }

  @Test
  public void testQueryWithFacetsWithWildCard() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.query = new Query();
    blurQuery.query.query = "test-family.testcol1:value1";
    blurQuery.query.rowQuery = true;
    blurQuery.query.scoreType = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = "1";
    blurQuery.facets = Arrays.asList(new Facet("test-family.testcol1:value*", Long.MAX_VALUE), new Facet(
        "test-family.testcol1:value-nohit", Long.MAX_VALUE));

    AtomicLongArray facetedCounts = new AtomicLongArray(2);
    BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, facetedCounts);
    assertEquals(iterable.getTotalResults(), 2);
    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult result = iterator.next();
      Selector selector = new Selector().setLocationId(result.getLocationId());
      FetchResult fetchResult = new FetchResult();
      indexManager.fetchRow(TABLE, selector, fetchResult);
      assertNotNull(fetchResult.rowResult);
      assertNull(fetchResult.recordResult);
    }

    assertEquals(2, facetedCounts.get(0));
    assertEquals(0, facetedCounts.get(1));

    assertFalse(indexManager.currentQueries(TABLE).isEmpty());
    Thread.sleep(2000);// wait for cleanup to fire
    assertTrue(indexManager.currentQueries(TABLE).isEmpty());
  }

  @Test
  public void testFetchRowByLocationId() throws Exception {
    Selector selector = new Selector().setLocationId(SHARD_NAME + "/0");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow(
        "row-1",
        newRecord(FAMILY, "record-1", newColumn("testcol1", "value1"), newColumn("testcol2", "value2"),
            newColumn("testcol3", "value3")));
    assertEquals(row, fetchResult.rowResult.row);
    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(1, rowResult.getTotalRecords());
  }

  @Test
  public void testFetchRowByLocationIdFromRecordOnlySearch() throws Exception {

    BlurQuery blurQuery = new BlurQuery();
    Query query = new Query();
    query.setQuery("recordid:record-5B");
    query.setRowQuery(false);
    blurQuery.setQuery(query);

    BlurResultIterable blurResultIterable = indexManager.query(TABLE, blurQuery, null);
    BlurIterator<BlurResult, BlurException> iterator = blurResultIterable.iterator();
    String locationId = null;
    if (iterator.hasNext()) {
      BlurResult result = iterator.next();
      locationId = result.locationId;
    } else {
      fail();
    }

    Selector selector = new Selector().setLocationId(locationId);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);

    Row row = newRow(
        "row-5",
        newRecord(FAMILY, "record-5A", newColumn("testcol1", "value13"), newColumn("testcol2", "value14"),
            newColumn("testcol3", "value15")),
        newRecord(FAMILY, "record-5B", newColumn("testcol1", "value16"), newColumn("testcol2", "value17"),
            newColumn("testcol3", "value18"), newColumn("testcol3", "value19")));
    assertEquals(row, fetchResult.rowResult.row);
    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(2, rowResult.getTotalRecords());
  }

  @Test
  public void testFetchMissingRowByLocationId() throws Exception {
    try {
      Selector selector = new Selector().setLocationId("shard4/0");
      FetchResult fetchResult = new FetchResult();
      indexManager.fetchRow(TABLE, selector, fetchResult);
      fail("Should throw exception");
    } catch (BlurException e) {
    }
  }

  @Test
  public void testFetchRecordByLocationId() throws Exception {
    Selector selector = new Selector().setLocationId(SHARD_NAME + "/0").setRecordOnly(true);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNull(fetchResult.rowResult);
    assertNotNull(fetchResult.recordResult.record);

    assertEquals("row-1", fetchResult.recordResult.rowid);
    assertEquals("record-1", fetchResult.recordResult.record.recordId);
    assertEquals(FAMILY, fetchResult.recordResult.record.family);

    Record record = newRecord(FAMILY, "record-1", newColumn("testcol1", "value1"), newColumn("testcol2", "value2"),
        newColumn("testcol3", "value3"));
    assertEquals(record, fetchResult.recordResult.record);
  }

  @Test
  public void testFetchRowByRowId() throws Exception {
    Selector selector = new Selector().setRowId("row-1");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow(
        "row-1",
        newRecord(FAMILY, "record-1", newColumn("testcol1", "value1"), newColumn("testcol2", "value2"),
            newColumn("testcol3", "value3")));
    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(1, rowResult.getTotalRecords());
  }

  @Test
  public void testFetchRowByRowIdWithInvalidFamily() throws Exception {
    Selector selector = new Selector().setRowId("row-1");
    selector.addToColumnFamiliesToFetch(UUID.randomUUID().toString());
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(new Row("row-1", null), rowResult.getRow());
    assertEquals(0, rowResult.getTotalRecords());
  }

  @Test
  public void testFetchRowByRowIdWithFamilySet() throws Exception {
    Selector selector = new Selector().setRowId("row-6");
    selector.addToColumnFamiliesToFetch(FAMILY2);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow("row-6", newRecord(FAMILY2, "record-6C", newColumn("testcol18", "value501")));
    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(1, rowResult.getTotalRecords());
  }

  @Test
  public void testFetchRowByRowIdWithColumnSet() throws Exception {
    Selector selector = new Selector().setRowId("row-6");
    selector.putToColumnsToFetch(FAMILY, new HashSet<String>(Arrays.asList("testcol12")));

    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow("row-6", newRecord(FAMILY, "record-6A", newColumn("testcol12", "value110")),
        newRecord(FAMILY, "record-6B", newColumn("testcol12", "value101")));
    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(2, rowResult.getTotalRecords());
  }

  @Test
  public void testFetchRowByRowIdWithFamilyAndColumnSet() throws Exception {
    Selector selector = new Selector().setRowId("row-6");
    selector.addToColumnFamiliesToFetch(FAMILY2);
    selector.putToColumnsToFetch(FAMILY, new HashSet<String>(Arrays.asList("testcol12")));
    selector.addToOrderOfFamiliesToFetch(FAMILY2);
    selector.addToOrderOfFamiliesToFetch(FAMILY);

    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow("row-6", newRecord(FAMILY2, "record-6C", newColumn("testcol18", "value501")),
        newRecord(FAMILY, "record-6A", newColumn("testcol12", "value110")),
        newRecord(FAMILY, "record-6B", newColumn("testcol12", "value101")));
    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(3, rowResult.getTotalRecords());
  }

  @Test
  public void testFetchRowByRowIdWithFilter() throws Exception {
    IndexManagerTestReadInterceptor.interceptor = new ReadInterceptor(null) {
      @Override
      public Filter getFilter() {
        return new QueryWrapperFilter(new TermQuery(new Term(FAMILY + ".testcol12", "value110")));
      }
    };
    Selector selector = new Selector().setRowId("row-6");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow("row-6",
        newRecord(FAMILY, "record-6A", newColumn("testcol12", "value110"), newColumn("testcol13", "value102")));

    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(1, rowResult.getTotalRecords());
  }

  @Test
  public void testFetchRowByRowIdWithFilterNoRow() throws Exception {
    IndexManagerTestReadInterceptor.interceptor = new ReadInterceptor(null) {
      @Override
      public Filter getFilter() {
        return new QueryWrapperFilter(new TermQuery(new Term(FAMILY + ".testcol12", "NOROW-1")));
      }
    };
    Selector selector = new Selector().setRowId("row-6");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertTrue(fetchResult.exists);
    assertFalse(fetchResult.deleted);
    assertNull(fetchResult.rowResult.row.records);
  }

  @Test
  public void testFetchRowByRowIdBatch() throws Exception {
    List<Selector> selectors = new ArrayList<Selector>();
    selectors.add(new Selector().setRowId("row-1"));
    selectors.add(new Selector().setRowId("row-2"));
    List<FetchResult> fetchRowBatch = indexManager.fetchRowBatch(TABLE, selectors);
    assertEquals(2, fetchRowBatch.size());
    FetchResult fetchResult1 = fetchRowBatch.get(0);
    assertNotNull(fetchResult1.rowResult.row);
    Row row1 = newRow(
        "row-1",
        newRecord(FAMILY, "record-1", newColumn("testcol1", "value1"), newColumn("testcol2", "value2"),
            newColumn("testcol3", "value3")));

    FetchRowResult rowResult1 = fetchResult1.getRowResult();
    assertEquals(row1, rowResult1.getRow());
    assertEquals(1, rowResult1.getTotalRecords());

    FetchResult fetchResult2 = fetchRowBatch.get(1);
    assertNotNull(fetchResult2.rowResult.row);
    Row row2 = newRow(
        "row-2",
        newRecord(FAMILY, "record-2", newColumn("testcol1", "value4"), newColumn("testcol2", "value5"),
            newColumn("testcol3", "value6")),
        newRecord(FAMILY, "record-2B", newColumn("testcol2", "value234123"), newColumn("testcol3", "value234123")));

    FetchRowResult rowResult2 = fetchResult2.getRowResult();
    assertEquals(row2, rowResult2.getRow());
    assertEquals(2, rowResult2.getTotalRecords());
  }

  @Test
  public void testFetchRowByRowIdPaging() throws Exception {
    Selector selector = new Selector().setRowId("row-6");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);

    List<Record> records = fetchResult.rowResult.row.getRecords();
    for (Record record : records) {
      System.out.println(record);
    }

    selector = new Selector().setRowId("row-6").setStartRecord(0).setMaxRecordsToFetch(1);
    fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    assertTrue(fetchResult.rowResult.moreRecordsToFetch);
    assertEquals(0, fetchResult.rowResult.startRecord);
    assertEquals(1, fetchResult.rowResult.maxRecordsToFetch);

    Row row1 = newRow("row-6",
        newRecord(FAMILY, "record-6A", newColumn("testcol12", "value110"), newColumn("testcol13", "value102")));
    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row1, rowResult.getRow());
    assertEquals(3, rowResult.getTotalRecords());

    selector = new Selector().setRowId("row-6").setStartRecord(1).setMaxRecordsToFetch(1);
    fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    assertTrue(fetchResult.rowResult.moreRecordsToFetch);
    assertEquals(1, fetchResult.rowResult.startRecord);
    assertEquals(1, fetchResult.rowResult.maxRecordsToFetch);

    Row row2 = newRow("row-6",
        newRecord(FAMILY, "record-6B", newColumn("testcol12", "value101"), newColumn("testcol13", "value104")));
    FetchRowResult rowResult2 = fetchResult.getRowResult();
    assertEquals(row2, rowResult2.getRow());
    assertEquals(3, rowResult2.getTotalRecords());

    selector = new Selector().setRowId("row-6").setStartRecord(2).setMaxRecordsToFetch(1);
    fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    assertFalse(fetchResult.rowResult.moreRecordsToFetch);
    assertEquals(2, fetchResult.rowResult.startRecord);
    assertEquals(1, fetchResult.rowResult.maxRecordsToFetch);

    Row row3 = newRow("row-6", newRecord(FAMILY2, "record-6C", newColumn("testcol18", "value501")));

    FetchRowResult rowResult3 = fetchResult.getRowResult();
    assertEquals(row3, rowResult3.getRow());
    assertEquals(3, rowResult3.getTotalRecords());

    selector = new Selector().setRowId("row-6").setStartRecord(3).setMaxRecordsToFetch(1);
    fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNull(fetchResult.rowResult.row.records);
    assertEquals(3, fetchResult.rowResult.getTotalRecords());
  }

  @Test
  public void testFetchRowByRecordIdOnly() throws Exception {
    Selector selector = new Selector().setRecordId("record-1");
    FetchResult fetchResult = new FetchResult();
    try {
      indexManager.fetchRow(TABLE, selector, fetchResult);
      fail("Invalid selector should throw exception.");
    } catch (BlurException e) {
      // do nothing, this is a pass
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testFetchRowByRecordIdOnlyNoRecordOnly() throws Exception {
    Selector selector = new Selector().setRowId("row-1").setRecordId("record-1");
    FetchResult fetchResult = new FetchResult();
    try {
      indexManager.fetchRow(TABLE, selector, fetchResult);
      fail("Invalid selector should throw exception.");
    } catch (BlurException e) {
      // do nothing, this is a pass
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testFetchRowByRecordId() throws Exception {
    Selector selector = new Selector().setRowId("row-1").setRecordId("record-1").setRecordOnly(true);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertFalse(fetchResult.deleted);
    assertTrue(fetchResult.exists);
    assertEquals(TABLE, fetchResult.table);
    assertNull(fetchResult.rowResult);
    assertNotNull(fetchResult.recordResult);
    FetchRecordResult recordResult = fetchResult.recordResult;
    assertEquals(FAMILY, recordResult.record.family);
    assertEquals("record-1", recordResult.record.recordId);
    assertEquals("row-1", recordResult.rowid);

    Record record = newRecord(FAMILY, "record-1", newColumn("testcol1", "value1"), newColumn("testcol2", "value2"),
        newColumn("testcol3", "value3"));
    assertEquals(record, recordResult.record);

  }

  @Test
  public void testFetchRowByRecordIdWithFilterHit() throws Exception {
    IndexManagerTestReadInterceptor.interceptor = new ReadInterceptor(null) {
      @Override
      public Filter getFilter() {
        return new QueryWrapperFilter(new TermQuery(new Term(FAMILY + ".testcol1", "value1")));
      }
    };
    Selector selector = new Selector().setRowId("row-1").setRecordId("record-1").setRecordOnly(true);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertFalse(fetchResult.deleted);
    assertTrue(fetchResult.exists);
    assertEquals(TABLE, fetchResult.table);
    assertNull(fetchResult.rowResult);
    assertNotNull(fetchResult.recordResult);
    FetchRecordResult recordResult = fetchResult.recordResult;
    assertEquals(FAMILY, recordResult.record.family);
    assertEquals("record-1", recordResult.record.recordId);
    assertEquals("row-1", recordResult.rowid);

    Record record = newRecord(FAMILY, "record-1", newColumn("testcol1", "value1"), newColumn("testcol2", "value2"),
        newColumn("testcol3", "value3"));
    assertEquals(record, recordResult.record);

  }

  @Test
  public void testFetchRowByRecordIdWithFilterNoHit() throws Exception {
    IndexManagerTestReadInterceptor.interceptor = new ReadInterceptor(null) {
      @Override
      public Filter getFilter() {
        return new QueryWrapperFilter(new TermQuery(new Term(FAMILY + ".testcol1", "NOHIT")));
      }
    };
    Selector selector = new Selector().setRowId("row-1").setRecordId("record-1").setRecordOnly(true);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertFalse(fetchResult.deleted);
    assertFalse(fetchResult.exists);
    assertEquals(TABLE, fetchResult.table);
    assertNull(fetchResult.rowResult);
    assertNull(fetchResult.recordResult);
  }

  @Test
  public void testRecordFrequency() throws Exception {
    assertEquals(2, indexManager.recordFrequency(TABLE, FAMILY, "testcol1", "value1"));
    assertEquals(0, indexManager.recordFrequency(TABLE, FAMILY, "testcol1", "NO VALUE"));
  }

  @Test
  public void testQuerySuperQueryTrue() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.query = new Query();
    blurQuery.query.query = "test-family.testcol1:value1";
    blurQuery.query.rowQuery = true;
    blurQuery.query.scoreType = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = "1";

    BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
    assertEquals(2, iterable.getTotalResults());
    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult result = iterator.next();
      Selector selector = new Selector().setLocationId(result.getLocationId());
      FetchResult fetchResult = new FetchResult();
      indexManager.fetchRow(TABLE, selector, fetchResult);
      assertNotNull(fetchResult.rowResult);
      assertNull(fetchResult.recordResult);
    }

    assertFalse(indexManager.currentQueries(TABLE).isEmpty());
    Thread.sleep(2000);// wait for cleanup to fire
    assertTrue(indexManager.currentQueries(TABLE).isEmpty());
  }

  @Test
  public void testQuerySuperQueryTrueWithFilter() throws Exception {
    IndexManagerTestReadInterceptor.interceptor = new ReadInterceptor(null) {
      @Override
      public Filter getFilter() {
        return new QueryWrapperFilter(new TermQuery(new Term(FAMILY + ".testcol2", "value2")));
      }
    };
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.query = new Query();
    blurQuery.query.query = "test-family.testcol1:value1";
    blurQuery.query.rowQuery = true;
    blurQuery.query.scoreType = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = "1";

    BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
    assertEquals(1, iterable.getTotalResults());
    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult result = iterator.next();
      Selector selector = new Selector().setLocationId(result.getLocationId());
      FetchResult fetchResult = new FetchResult();
      indexManager.fetchRow(TABLE, selector, fetchResult);
      assertNotNull(fetchResult.rowResult);
      assertNull(fetchResult.recordResult);
    }

    assertFalse(indexManager.currentQueries(TABLE).isEmpty());
    Thread.sleep(2000);// wait for cleanup to fire
    assertTrue(indexManager.currentQueries(TABLE).isEmpty());
  }

  @Test
  public void testQuerySuperQueryTrueWithSelector() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.query = new Query();
    blurQuery.query.query = "test-family.testcol1:value1";
    blurQuery.query.rowQuery = true;
    blurQuery.query.scoreType = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = "1";
    blurQuery.selector = new Selector();

    BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
    assertEquals(iterable.getTotalResults(), 2);
    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult result = iterator.next();
      assertNotNull(result.fetchResult.rowResult);
      assertNull(result.fetchResult.recordResult);
    }

    assertFalse(indexManager.currentQueries(TABLE).isEmpty());
    Thread.sleep(2000);// wait for cleanup to fire
    assertTrue(indexManager.currentQueries(TABLE).isEmpty());
  }

  @Test
  public void testQuerySuperQueryFalse() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.query = new Query();
    blurQuery.query.query = "test-family.testcol1:value1";
    blurQuery.query.rowQuery = false;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = "1";

    BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
    assertEquals(iterable.getTotalResults(), 2);
    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult result = iterator.next();
      Selector selector = new Selector().setLocationId(result.getLocationId()).setRecordOnly(true);
      FetchResult fetchResult = new FetchResult();
      indexManager.fetchRow(TABLE, selector, fetchResult);
      assertNull(fetchResult.rowResult);
      assertNotNull(fetchResult.recordResult);
    }

    assertFalse(indexManager.currentQueries(TABLE).isEmpty());
    Thread.sleep(2000);// wait for cleanup to fire
    assertTrue(indexManager.currentQueries(TABLE).isEmpty());
  }

  @Test
  public void testQuerySuperQueryFalseWithSelector() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.query = new Query();
    blurQuery.query.query = "test-family.testcol1:value1";
    blurQuery.query.rowQuery = false;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = "1";
    blurQuery.selector = new Selector();
    blurQuery.selector.setRecordOnly(true);

    BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
    assertEquals(iterable.getTotalResults(), 2);
    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult result = iterator.next();
      assertNull(result.fetchResult.rowResult);
      assertNotNull(result.fetchResult.recordResult);
    }

    assertFalse(indexManager.currentQueries(TABLE).isEmpty());
    Thread.sleep(2000);// wait for cleanup to fire
    assertTrue(indexManager.currentQueries(TABLE).isEmpty());
  }

  @Test
  public void testQueryRecordOnly() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.query = new Query();
    blurQuery.query.query = "test-family.testcol1:value1";
    blurQuery.selector = new Selector();
    blurQuery.selector.setRecordOnly(true);

    BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, null);
    assertEquals(iterable.getTotalResults(), 2);

    int matchRecord1 = 0;
    int matchRecord4 = 0;

    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult result = iterator.next();
      assertNull(result.fetchResult.rowResult);
      assertNotNull(result.fetchResult.recordResult);

      Record r = result.fetchResult.recordResult.record;

      if (r.getRecordId().equals("record-1")) {
        matchRecord1 += 1;
      } else if (r.getRecordId().equals("record-4")) {
        matchRecord4 += 1;
      } else {
        fail("Unexpected record ID [" + r.getRecordId() + "]");
      }
    }

    assertEquals("Unexpected number of record-1 results", 1, matchRecord1);
    assertEquals("Unexpected number of record-4 results", 1, matchRecord4);
  }

  @Test
  public void testQueryWithFacets() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.query = new Query();
    blurQuery.query.query = "test-family.testcol1:value1";
    blurQuery.query.rowQuery = true;
    blurQuery.query.scoreType = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = "1";
    blurQuery.facets = Arrays.asList(new Facet("test-family.testcol1:value1", Long.MAX_VALUE), new Facet(
        "test-family.testcol1:value-nohit", Long.MAX_VALUE));

    AtomicLongArray facetedCounts = new AtomicLongArray(2);
    BlurResultIterable iterable = indexManager.query(TABLE, blurQuery, facetedCounts);
    assertEquals(iterable.getTotalResults(), 2);
    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult result = iterator.next();
      Selector selector = new Selector().setLocationId(result.getLocationId());
      FetchResult fetchResult = new FetchResult();
      indexManager.fetchRow(TABLE, selector, fetchResult);
      assertNotNull(fetchResult.rowResult);
      assertNull(fetchResult.recordResult);
    }

    assertEquals(2, facetedCounts.get(0));
    assertEquals(0, facetedCounts.get(1));

    assertFalse(indexManager.currentQueries(TABLE).isEmpty());
    Thread.sleep(2000);// wait for cleanup to fire
    assertTrue(indexManager.currentQueries(TABLE).isEmpty());
  }

  @Test
  public void testTerms() throws Exception {
    List<String> terms = indexManager.terms(TABLE, FAMILY, "testcol1", "", (short) 100);
    assertEquals(Arrays.asList("value1", "value13", "value16", "value4", "value7"), terms);
  }

  @Test
  public void testTerms2() throws Exception {
    List<String> terms = indexManager.terms(TABLE, FAMILY, "rowid", "", (short) 100);
    assertEquals(Arrays.asList("row-1", "row-2", "row-3", "row-4", "row-5", "row-6", "row-7"), terms);
  }

  @Test
  public void testTermsNonExistentField() throws Exception {
    List<String> terms = indexManager.terms(TABLE, FAMILY, "nonexistentfield", "", (short) 100);
    assertNotNull("Non-existent fields should not return null.", terms);
    assertEquals("The terms of non-existent fields should be empty.", 0, terms.size());
  }

  @Test
  public void testMutationReplaceRow() throws Exception {
    RowMutation mutation = newRowMutation(
        TABLE,
        "row-4",
        newRecordMutation(FAMILY, "record-4", newColumn("testcol1", "value2"), newColumn("testcol2", "value3"),
            newColumn("testcol3", "value4")));
    indexManager.mutate(mutation);

    Selector selector = new Selector().setRowId("row-4");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow(
        "row-4",
        newRecord(FAMILY, "record-4", newColumn("testcol1", "value2"), newColumn("testcol2", "value3"),
            newColumn("testcol3", "value4")));

    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(1, rowResult.getTotalRecords());
  }

  @Test
  public void testMutationReplaceRowFailureWithNullRecordId() throws Exception {
    RowMutation mutation = newRowMutation(
        TABLE,
        "row-4",
        newRecordMutation(FAMILY, null, newColumn("testcol1", "value2"), newColumn("testcol2", "value3"),
            newColumn("testcol3", "value4")));
    try {
      indexManager.mutate(mutation);
      fail();
    } catch (BlurException e) {
    }
  }

  @Test
  public void testMutationReplaceRowWithNullRowId() throws Exception {
    RowMutation mutation = newRowMutation(
        TABLE,
        null,
        newRecordMutation(FAMILY, "record-4", newColumn("testcol1", "value2"), newColumn("testcol2", "value3"),
            newColumn("testcol3", "value4")));
    try {
      indexManager.mutate(mutation);
      fail();
    } catch (BlurException e) {
    }
  }

  @Test
  public void testMultipleMutationReplaceRecordWithInSameBatch() throws Exception {
    RowMutation mutation1 = newRowMutation(
        TABLE,
        "row-4000",
        newRecordMutation(FAMILY, "record-4a", newColumn("testcol1", "value2"), newColumn("testcol2", "value3"),
            newColumn("testcol3", "value4")));

    RowMutation mutation2 = newRowMutation(
        TABLE,
        "row-4000",
        newRecordMutation(FAMILY, "record-4b", newColumn("testcol1", "value2"), newColumn("testcol2", "value3"),
            newColumn("testcol3", "value4")));
    mutation1.setRowMutationType(RowMutationType.UPDATE_ROW);
    mutation2.setRowMutationType(RowMutationType.UPDATE_ROW);
    indexManager.mutate(Arrays.asList(mutation1, mutation2));
    Selector selector = new Selector().setRowId("row-4000");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow(
        "row-4000",
        newRecord(FAMILY, "record-4a", newColumn("testcol1", "value2"), newColumn("testcol2", "value3"),
            newColumn("testcol3", "value4")),
        newRecord(FAMILY, "record-4b", newColumn("testcol1", "value2"), newColumn("testcol2", "value3"),
            newColumn("testcol3", "value4")));

    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(2, rowResult.getTotalRecords());
  }

  @Test
  public void testMutationReplaceMissingRow() throws Exception {
    Column c1 = newColumn("testcol1", "value20");
    Column c2 = newColumn("testcol2", "value21");
    Column c3 = newColumn("testcol3", "value22");
    String rec = "record-6";
    RecordMutation rm = newRecordMutation(FAMILY, rec, c1, c2, c3);
    RowMutation mutation = newRowMutation(TABLE, "row-6", rm);
    indexManager.mutate(mutation);

    Selector selector = new Selector().setRowId("row-6");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    Row r = fetchResult.rowResult.row;
    assertNotNull("new row should exist", r);
    Row row = newRow(
        "row-6",
        newRecord(FAMILY, "record-6", newColumn("testcol1", "value20"), newColumn("testcol2", "value21"),
            newColumn("testcol3", "value22")));

    FetchRowResult rowResult = fetchResult.getRowResult();
    assertEquals(row, rowResult.getRow());
    assertEquals(1, rowResult.getTotalRecords());
  }

  @Test
  public void testMutationDeleteRow() throws Exception {
    RowMutation mutation = newRowMutation(DELETE_ROW, TABLE, "row-2");
    indexManager.mutate(mutation);

    Selector selector = new Selector().setRowId("row-2");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNull("row should be deleted", fetchResult.rowResult);
  }

  @Test
  public void testMutationDeleteMissingRow() throws Exception {
    RowMutation mutation = newRowMutation(DELETE_ROW, TABLE, "row-6");
    indexManager.mutate(mutation);

    Selector selector = new Selector().setRowId("row-6");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNull("row should not exist", fetchResult.rowResult);
  }

  @Test
  public void testMutationUpdateRowDeleteLastRecord() throws Exception {
    RecordMutation rm = newRecordMutation(DELETE_ENTIRE_RECORD, FAMILY, "record-3");

    RowMutation rowMutation = newRowMutation(UPDATE_ROW, TABLE, "row-3", rm);

    indexManager.mutate(rowMutation);

    Selector selector = new Selector().setRowId("row-3");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNull("row should not exist", fetchResult.rowResult);
  }

  @Test
  public void testMutationUpdateRowDeleteRecord() throws Exception {
    RecordMutation rm = newRecordMutation(DELETE_ENTIRE_RECORD, FAMILY, "record-5A");

    RowMutation rowMutation = newRowMutation(UPDATE_ROW, TABLE, "row-5", rm);
    indexManager.mutate(rowMutation);

    Selector selector = new Selector().setRowId("row-5");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull("row should exist", fetchResult.rowResult);
    assertNotNull("row should exist", fetchResult.rowResult.row);
    assertEquals("row should have one record", 1, fetchResult.rowResult.row.getRecordsSize());
  }

  @Test
  public void testMutationUpdateRowReplaceExistingRecord() throws Exception {
    Column c1 = newColumn("testcol4", "value104");
    Column c2 = newColumn("testcol5", "value105");
    Column c3 = newColumn("testcol6", "value105");
    String rec = "record-5A";
    RecordMutation rm = newRecordMutation(REPLACE_ENTIRE_RECORD, FAMILY, rec, c1, c2, c3);

    Record r = updateAndFetchRecord("row-5", rec, rm);

    assertNotNull("record should exist", r);
    assertEquals("only 3 columns in record", 3, r.getColumnsSize());
    assertTrue("column 1 should be in record", r.columns.contains(c1));
    assertTrue("column 2 should be in record", r.columns.contains(c2));
    assertTrue("column 3 should be in record", r.columns.contains(c3));
  }

  @Test
  public void testMutationUpdateRowReplaceMissingRecord() throws Exception {
    Column c1 = newColumn("testcol4", "value104");
    Column c2 = newColumn("testcol5", "value105");
    Column c3 = newColumn("testcol6", "value105");
    String rec = "record-5C";
    RecordMutation rm = newRecordMutation(REPLACE_ENTIRE_RECORD, FAMILY, rec, c1, c2, c3);

    Record r = updateAndFetchRecord("row-5", rec, rm);

    assertNotNull("record should exist", r);
    assertEquals("only 3 columns in record", 3, r.getColumnsSize());
    assertTrue("column 1 should be in record", r.columns.contains(c1));
    assertTrue("column 2 should be in record", r.columns.contains(c2));
    assertTrue("column 3 should be in record", r.columns.contains(c3));
  }

  @Test
  public void testMutationUpdateRowReplaceMixedRecords() throws Exception {
    Column c1 = newColumn("testcol4", "value104");
    Column c2 = newColumn("testcol5", "value105");
    Column c3 = newColumn("testcol6", "value105");
    RecordMutation rm1 = newRecordMutation(REPLACE_ENTIRE_RECORD, FAMILY, "record-5A", c1, c2, c3);
    Column c4 = newColumn("testcol4", "value104");
    Column c5 = newColumn("testcol5", "value105");
    Column c6 = newColumn("testcol6", "value105");
    RecordMutation rm2 = newRecordMutation(REPLACE_ENTIRE_RECORD, FAMILY, "record-5C", c4, c5, c6);

    RowMutation rowMutation = newRowMutation(UPDATE_ROW, TABLE, "row-5", rm1, rm2);
    indexManager.mutate(rowMutation);

    Selector selector = new Selector().setRowId("row-5");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    Row r = fetchResult.rowResult.row;
    assertNotNull("row should exist", r);
    assertEquals("only 3 records in row", 3, r.getRecordsSize());
    int rm1Matches = 0;
    int rm2Matches = 0;
    int nonMatches = 0;
    for (Record record : r.records) {
      if (match(rm1, record)) {
        rm1Matches += 1;
      } else if (match(rm2, record)) {
        rm2Matches += 1;
      } else {
        nonMatches += 1;
      }
    }
    assertEquals("matching record should be updated", 1, rm1Matches);
    assertEquals("missing record should be added", 1, rm2Matches);
    assertEquals("unmodified record should exist", 1, nonMatches);
  }

  @Test
  public void testMutationUpdateRowReplaceExistingColumns() throws Exception {
    Column c1 = newColumn("testcol1", "value999");
    Column c2 = newColumn("testcol2", "value9999");
    String rec = "record-1";
    RecordMutation rm = newRecordMutation(REPLACE_COLUMNS, FAMILY, rec, c1, c2);

    Record r = updateAndFetchRecord("row-1", rec, rm);

    assertNotNull("record should exist", r);
    assertEquals("only 3 columns in record", 3, r.getColumnsSize());
    assertTrue("column 1 should be in record", r.columns.contains(c1));
    assertTrue("column 2 should be in record", r.columns.contains(c2));
    boolean foundUnmodifiedColumn = false;
    for (Column column : r.columns) {
      if (column.name.equals("testcol3") && column.value.equals("value3")) {
        foundUnmodifiedColumn = true;
        break;
      }
    }
    assertTrue("column 3 should be unmodified", foundUnmodifiedColumn);
  }

  @Test
  public void testMutationUpdateRowReplaceExistingColumnsWhileDeletingAColumn() throws Exception {
    Column c1 = newColumn("testcol1", "value999");
    Column c2 = newColumn("testcol2", null);
    String rec = "record-1";
    RecordMutation rm = newRecordMutation(REPLACE_COLUMNS, FAMILY, rec, c1, c2);

    Record r = updateAndFetchRecord("row-1", rec, rm);

    assertNotNull("record should exist", r);
    assertEquals("only 2 columns in record", 2, r.getColumnsSize());
    assertTrue("column 1 should be in record", r.columns.contains(c1));
    boolean foundUnmodifiedColumn = false;
    for (Column column : r.columns) {
      if (column.name.equals("testcol3") && column.value.equals("value3")) {
        foundUnmodifiedColumn = true;
        break;
      }
    }
    assertTrue("column 3 should be unmodified", foundUnmodifiedColumn);
  }

  @Test
  public void testMutationUpdateRowReplaceExistingDuplicateColumns() throws Exception {
    Column c = newColumn("testcol3", "value999");
    String rec = "record-5B";
    RecordMutation rm = newRecordMutation(REPLACE_COLUMNS, FAMILY, rec, c);

    Record r = updateAndFetchRecord("row-5", rec, rm);

    assertNotNull("record should exist", r);
    assertEquals("only 3 columns in record", 3, r.getColumnsSize());
    assertTrue("new column should be in record", r.columns.contains(c));
    boolean foundDuplicateColumn = false;
    for (Column column : r.columns) {
      if (column.name.equals(c.name) && !column.value.equals(c.value)) {
        foundDuplicateColumn = true;
        break;
      }
    }
    assertFalse("duplicate columns should be removed", foundDuplicateColumn);
  }

  @Test
  public void testMutationUpdateRowReplaceMissingColumns() throws Exception {
    Column c1 = newColumn("testcol4", "value999");
    Column c2 = newColumn("testcol5", "value9999");
    String rec = "record-1";
    RecordMutation rm = newRecordMutation(REPLACE_COLUMNS, FAMILY, rec, c1, c2);

    Record r = updateAndFetchRecord("row-1", rec, rm);

    assertNotNull("record should exist", r);
    assertEquals("only 5 columns in record", 5, r.getColumnsSize());
    assertTrue("column 1 should be in record", r.columns.contains(c1));
    assertTrue("column 2 should be in record", r.columns.contains(c2));
  }

  @Test
  public void testMutationUpdateRowReplaceMixedColumns() throws Exception {
    Column c1 = newColumn("testcol1", "value999");
    Column c2 = newColumn("testcol4", "value9999");
    String rec = "record-1";
    RecordMutation rm = newRecordMutation(REPLACE_COLUMNS, FAMILY, rec, c1, c2);

    Record r = updateAndFetchRecord("row-1", rec, rm);

    assertNotNull("record should exist", r);
    assertEquals("only 4 columns in record", 4, r.getColumnsSize());
    assertTrue("column 1 should be in record", r.columns.contains(c1));
    assertTrue("column 2 should be in record", r.columns.contains(c2));
  }

  @Test
  public void testMutationUpdateRowMissingRecordReplaceColumns() throws Exception {
    Column c1 = newColumn("testcol4", "value999");
    Column c2 = newColumn("testcol5", "value9999");
    String rec = "record-1B";
    RecordMutation rm = newRecordMutation(REPLACE_COLUMNS, FAMILY, rec, c1, c2);

    Record r = updateAndFetchRecord("row-1", rec, rm);
    assertNotNull("record should exist", r);
    assertEquals("only 2 columns in record", 2, r.getColumnsSize());
    assertTrue("column 1 should be in record", r.columns.contains(c1));
    assertTrue("column 2 should be in record", r.columns.contains(c2));
  }

  @Test
  public void testMutationUpdateMissingRowReplaceColumns() throws Exception {
    Column c1 = newColumn("testcol1", "value999");
    Column c2 = newColumn("testcol2", "value9999");
    String rec = "record-6";
    RecordMutation rm = newRecordMutation(REPLACE_COLUMNS, FAMILY, rec, c1, c2);

    Record r = updateAndFetchRecord("row-6", rec, rm);
    assertNotNull("record should exist", r);
    assertEquals("only 2 columns in record", 2, r.getColumnsSize());
    assertTrue("column 1 should be in record", r.columns.contains(c1));
    assertTrue("column 2 should be in record", r.columns.contains(c2));
  }

  @Test
  public void testMutationUpdateRowAppendColumns() throws Exception {
    Column c1 = newColumn("testcol1", "value999");
    Column c2 = newColumn("testcol2", "value9999");
    Column c3 = newColumn("testcol4", "hmm");
    String rec = "record-1";
    RecordMutation rm = newRecordMutation(APPEND_COLUMN_VALUES, FAMILY, rec, c1, c2, c3);

    Record r = updateAndFetchRecord("row-1", rec, rm);

    assertNotNull("record should exist", r);
    assertEquals("only 6 columns in record", 6, r.getColumnsSize());
    assertTrue("column 1 should be in record", r.columns.contains(c1));
    assertTrue("column 2 should be in record", r.columns.contains(c2));
    assertTrue("column 3 should be in record", r.columns.contains(c3));
    int numTestcol1 = 0;
    int numTestcol2 = 0;
    int numTestcol3 = 0;
    int numTestcol4 = 0;
    int others = 0;
    for (Column column : r.columns) {
      if (column.name.equals("testcol1")) {
        numTestcol1 += 1;
      } else if (column.name.equals("testcol2")) {
        numTestcol2 += 1;
      } else if (column.name.equals("testcol3")) {
        numTestcol3 += 1;
      } else if (column.name.equals("testcol4")) {
        numTestcol4 += 1;
      } else {
        others += 1;
      }
    }
    assertEquals("should append testcol1", 2, numTestcol1);
    assertEquals("should append testcol2", 2, numTestcol2);
    assertEquals("should not append testcol3", 1, numTestcol3);
    assertEquals("should append testcol4", 1, numTestcol4);
    assertEquals("should not find other columns", 0, others);
  }

  @Test
  public void testMutationUpdateRowMissingRecordAppendColumns() throws Exception {
    Column c1 = newColumn("testcol1", "value999");
    Column c2 = newColumn("testcol2", "value9999");
    Column c3 = newColumn("testcol4", "hmm");
    String rec = "record-1B";
    RecordMutation rm = newRecordMutation(APPEND_COLUMN_VALUES, FAMILY, rec, c1, c2, c3);

    Record r = updateAndFetchRecord("row-1", rec, rm);
    assertNotNull("record should exist", r);
    assertEquals("only 3 columns in record", 3, r.getColumnsSize());
    assertTrue("column 1 should be in record", r.columns.contains(c1));
    assertTrue("column 2 should be in record", r.columns.contains(c2));
    assertTrue("column 3 should be in record", r.columns.contains(c3));
  }

  @Test
  public void testMutationUpdateMissingRowAppendColumns() throws Exception {
    Column c1 = newColumn("testcol1", "value999");
    Column c2 = newColumn("testcol2", "value9999");
    String rec = "record-6";
    RecordMutation rm = newRecordMutation(APPEND_COLUMN_VALUES, FAMILY, rec, c1, c2);

    Record r = updateAndFetchRecord("row-6", rec, rm);
    assertNotNull("record should exist", r);
    assertEquals("only 2 columns in record", 2, r.getColumnsSize());
    assertTrue("column 1 should be in record", r.columns.contains(c1));
    assertTrue("column 2 should be in record", r.columns.contains(c2));
  }

  private Record updateAndFetchRecord(String rowId, String recordId, RecordMutation... recordMutations)
      throws Exception {
    RowMutation rowMutation = newRowMutation(UPDATE_ROW, TABLE, rowId, recordMutations);
    indexManager.mutate(rowMutation);

    Selector selector = new Selector().setRowId(rowId).setRecordId(recordId);
    selector.setRecordOnly(true);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    return (fetchResult.recordResult != null ? fetchResult.recordResult.record : null);
  }

}
