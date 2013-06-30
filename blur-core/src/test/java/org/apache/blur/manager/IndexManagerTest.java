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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.blur.manager.clusterstatus.ClusterStatus;
import org.apache.blur.manager.indexserver.LocalIndexServer;
import org.apache.blur.manager.results.BlurIterator;
import org.apache.blur.manager.results.BlurResultIterable;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Facet;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.HighlightOptions;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.SimpleQuery;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexManagerTest {

  private static final File TMPDIR = new File("./target/tmp");

  private static final String SHARD_NAME = BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, 0);
  private static final String TABLE = "table";
  private static final String FAMILY = "test-family";
  private static final String FAMILY2 = "test-family2";
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

    final TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(TABLE);
    tableDescriptor.setTableUri(file.toURI().toString());
    tableDescriptor.setAnalyzerDefinition(new AnalyzerDefinition());
    tableDescriptor.putToTableProperties("blur.shard.time.between.refreshs", Long.toString(100));
    tableDescriptor.setShardCount(1);
    server = new LocalIndexServer(tableDescriptor);

    indexManager = new IndexManager();
    indexManager.setStatusCleanupTimerDelay(1000);
    indexManager.setIndexServer(server);
    indexManager.setThreadCount(1);
    indexManager.setClusterStatus(new ClusterStatus() {

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
      public boolean isBlockCacheEnabled(String cluster, String table) {
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
      public int getShardCount(boolean useCache, String cluster, String table) {
        throw new RuntimeException("Not impl");
      }

      @Override
      public List<String> getOnlineShardServers(boolean useCache, String cluster) {
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
      public Set<String> getBlockCacheFileTypes(String cluster, String table) {
        throw new RuntimeException("Not impl");
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
    });
    indexManager.init();
    setupData();
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
    mutation7.waitToBeVisible = true;
    indexManager.mutate(mutation1);
    indexManager.mutate(mutation2);
    indexManager.mutate(mutation3);
    indexManager.mutate(mutation4);
    indexManager.mutate(mutation5);
    indexManager.mutate(mutation6);
    indexManager.mutate(mutation7);
  }

  @Test
  public void testFetchRowByRowIdHighlighting() throws Exception {
    Selector selector = new Selector().setRowId("row-6");
    HighlightOptions highlightOptions = new HighlightOptions();
    SimpleQuery simpleQuery = new SimpleQuery();
    simpleQuery.setQueryStr(FAMILY2 + ".testcol13:value105 " + FAMILY + ".testcol12:value101");
    highlightOptions.setSimpleQuery(simpleQuery);
    selector.setHighlightOptions(highlightOptions);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);

    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow("row-6", newRecord(FAMILY, "record-6B", newColumn("testcol12", "<<<value101>>>")));
    row.recordCount = 3;
    assertEquals(row, fetchResult.rowResult.row);
  }
  
  @Test
  public void testFetchRowByRowIdHighlightingWithFullText() throws Exception {
    Selector selector = new Selector().setRowId("row-6");
    HighlightOptions highlightOptions = new HighlightOptions();
    SimpleQuery simpleQuery = new SimpleQuery();
    simpleQuery.setQueryStr("cool value101");
    highlightOptions.setSimpleQuery(simpleQuery);
    selector.setHighlightOptions(highlightOptions);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);

    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow("row-6", newRecord(FAMILY, "record-6B", newColumn("testcol12", "<<<value101>>>")));
    row.recordCount = 3;
    assertEquals(row, fetchResult.rowResult.row);
  }
  
  @Test
  public void testFetchRowByRowIdHighlightingWithFullTextWildCard() throws Exception {
    Selector selector = new Selector().setRowId("row-6");
    HighlightOptions highlightOptions = new HighlightOptions();
    SimpleQuery simpleQuery = new SimpleQuery();
    simpleQuery.setQueryStr("cool ?alue101");
    highlightOptions.setSimpleQuery(simpleQuery);
    selector.setHighlightOptions(highlightOptions);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);

    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow("row-6", newRecord(FAMILY, "record-6B", newColumn("testcol12", "<<<value101>>>")));
    row.recordCount = 3;
    assertEquals(row, fetchResult.rowResult.row);
  }

  @Test
  public void testFetchRecordByLocationIdHighlighting() throws Exception {
    Selector selector = new Selector().setLocationId(SHARD_NAME + "/0").setRecordOnly(true);
    HighlightOptions highlightOptions = new HighlightOptions();
    SimpleQuery simpleQuery = new SimpleQuery();
    simpleQuery.setQueryStr(FAMILY + ".testcol1:value1");
    simpleQuery.setSuperQueryOn(false);
    highlightOptions.setSimpleQuery(simpleQuery);
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
    blurQuery.simpleQuery = new SimpleQuery();
    blurQuery.simpleQuery.queryStr = "+super:<+test-family.testcol12:value101 +test-family.testcol13:value102> +super:<test-family2.testcol18:value501>";

    blurQuery.simpleQuery.superQueryOn = true;
    blurQuery.simpleQuery.type = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = 1;

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
    blurQuery.simpleQuery = new SimpleQuery();
    blurQuery.simpleQuery.queryStr = "+super:<+test-family.testcol12:value101 +test-family.testcol13:value102> +super:<test-family2.testcol18:value501>";
    blurQuery.simpleQuery.superQueryOn = true;
    blurQuery.simpleQuery.type = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = 1;

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
    blurQuery.simpleQuery = new SimpleQuery();
    blurQuery.simpleQuery.queryStr = "+super:<test-family.testcol1:value1> +super:<test-family.testcol3:value234123>";
    blurQuery.simpleQuery.superQueryOn = true;
    blurQuery.simpleQuery.type = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = 1;

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
    blurQuery.simpleQuery = new SimpleQuery();
    blurQuery.simpleQuery.queryStr = "test-family.testcol1:value1";
    blurQuery.simpleQuery.superQueryOn = true;
    blurQuery.simpleQuery.type = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = 1;
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
    row.recordCount = 1;
    assertEquals(row, fetchResult.rowResult.row);
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
    row.recordCount = 1;
    assertEquals(row, fetchResult.rowResult.row);
  }

  @Test
  public void testFetchRowByRowIdPaging() throws Exception {
    Selector selector = new Selector().setRowId("row-6").setStartRecord(0).setMaxRecordsToFetch(1);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);

    Row row1 = newRow("row-6",
        newRecord(FAMILY, "record-6A", newColumn("testcol12", "value110"), newColumn("testcol13", "value102")));
    row1.recordCount = 1;
    assertEquals(row1, fetchResult.rowResult.row);

    selector = new Selector().setRowId("row-6").setStartRecord(1).setMaxRecordsToFetch(1);
    fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);

    Row row2 = newRow("row-6",
        newRecord(FAMILY, "record-6B", newColumn("testcol12", "value101"), newColumn("testcol13", "value104")));
    row2.recordCount = 1;
    assertEquals(row2, fetchResult.rowResult.row);

    selector = new Selector().setRowId("row-6").setStartRecord(2).setMaxRecordsToFetch(1);
    fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);

    Row row3 = newRow("row-6", newRecord(FAMILY2, "record-6C", newColumn("testcol18", "value501")));
    row3.recordCount = 1;
    assertEquals(row3, fetchResult.rowResult.row);

    selector = new Selector().setRowId("row-6").setStartRecord(3).setMaxRecordsToFetch(1);
    fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNull(fetchResult.rowResult.row);
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
  public void testRecordFrequency() throws Exception {
    assertEquals(2, indexManager.recordFrequency(TABLE, FAMILY, "testcol1", "value1"));
    assertEquals(0, indexManager.recordFrequency(TABLE, FAMILY, "testcol1", "NO VALUE"));
  }

  @Test
  public void testSchema() throws Exception {
    Schema schema = indexManager.schema(TABLE);
    assertEquals(TABLE, schema.table);
    Map<String, Set<String>> columnFamilies = schema.columnFamilies;
    assertEquals(new TreeSet<String>(Arrays.asList(FAMILY, FAMILY2)), new TreeSet<String>(columnFamilies.keySet()));
    assertEquals(new TreeSet<String>(Arrays.asList("testcol1", "testcol2", "testcol3", "testcol12", "testcol13")),
        new TreeSet<String>(columnFamilies.get(FAMILY)));
    assertEquals(new TreeSet<String>(Arrays.asList("testcol18")), new TreeSet<String>(columnFamilies.get(FAMILY2)));
  }

  @Test
  public void testQuerySuperQueryTrue() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.simpleQuery = new SimpleQuery();
    blurQuery.simpleQuery.queryStr = "test-family.testcol1:value1";
    blurQuery.simpleQuery.superQueryOn = true;
    blurQuery.simpleQuery.type = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = 1;

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
  public void testQuerySuperQueryTrueWithSelector() throws Exception {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.simpleQuery = new SimpleQuery();
    blurQuery.simpleQuery.queryStr = "test-family.testcol1:value1";
    blurQuery.simpleQuery.superQueryOn = true;
    blurQuery.simpleQuery.type = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = 1;
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
    blurQuery.simpleQuery = new SimpleQuery();
    blurQuery.simpleQuery.queryStr = "test-family.testcol1:value1";
    blurQuery.simpleQuery.superQueryOn = false;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = 1;

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
    blurQuery.simpleQuery = new SimpleQuery();
    blurQuery.simpleQuery.queryStr = "test-family.testcol1:value1";
    blurQuery.simpleQuery.superQueryOn = false;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = 1;
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
    blurQuery.simpleQuery = new SimpleQuery();
    blurQuery.simpleQuery.queryStr = "test-family.testcol1:value1";
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
    blurQuery.simpleQuery = new SimpleQuery();
    blurQuery.simpleQuery.queryStr = "test-family.testcol1:value1";
    blurQuery.simpleQuery.superQueryOn = true;
    blurQuery.simpleQuery.type = ScoreType.SUPER;
    blurQuery.fetch = 10;
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = 1;
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
  public void testMutationReplaceRow() throws Exception {
    RowMutation mutation = newRowMutation(
        TABLE,
        "row-4",
        newRecordMutation(FAMILY, "record-4", newColumn("testcol1", "value2"), newColumn("testcol2", "value3"),
            newColumn("testcol3", "value4")));
    mutation.waitToBeVisible = true;
    indexManager.mutate(mutation);

    Selector selector = new Selector().setRowId("row-4");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull(fetchResult.rowResult.row);
    Row row = newRow(
        "row-4",
        newRecord(FAMILY, "record-4", newColumn("testcol1", "value2"), newColumn("testcol2", "value3"),
            newColumn("testcol3", "value4")));
    row.recordCount = 1;
    assertEquals(row, fetchResult.rowResult.row);
  }

  @Test
  public void testMutationReplaceMissingRow() throws Exception {
    Column c1 = newColumn("testcol1", "value20");
    Column c2 = newColumn("testcol2", "value21");
    Column c3 = newColumn("testcol3", "value22");
    String rec = "record-6";
    RecordMutation rm = newRecordMutation(FAMILY, rec, c1, c2, c3);
    RowMutation mutation = newRowMutation(TABLE, "row-6", rm);
    mutation.waitToBeVisible = true;
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
    row.recordCount = 1;
    assertEquals("row should match", row, r);
  }

  @Test
  public void testMutationDeleteRow() throws Exception {
    RowMutation mutation = newRowMutation(DELETE_ROW, TABLE, "row-2");
    mutation.waitToBeVisible = true;
    indexManager.mutate(mutation);

    Selector selector = new Selector().setRowId("row-2");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNull("row should be deleted", fetchResult.rowResult);
  }

  @Test
  public void testMutationDeleteMissingRow() throws Exception {
    RowMutation mutation = newRowMutation(DELETE_ROW, TABLE, "row-6");
    mutation.waitToBeVisible = true;
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

    rowMutation.waitToBeVisible = true;
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
    rowMutation.waitToBeVisible = true;
    indexManager.mutate(rowMutation);

    Selector selector = new Selector().setRowId("row-5");
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    assertNotNull("row should exist", fetchResult.rowResult);
    assertNotNull("row should exist", fetchResult.rowResult.row);
    assertEquals("row should have one record", 1, fetchResult.rowResult.row.getRecordsSize());
  }

  @Test(expected = BlurException.class)
  public void testMutationUpdateMissingRowDeleteRecord() throws Exception {
    RecordMutation rm = newRecordMutation(DELETE_ENTIRE_RECORD, FAMILY, "record-101");

    RowMutation rowMutation = newRowMutation(UPDATE_ROW, TABLE, "row-101", rm);
    rowMutation.waitToBeVisible = true;
    indexManager.mutate(rowMutation);
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
    rowMutation.waitToBeVisible = true;
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

  @Test(expected = BlurException.class)
  public void testMutationUpdateMissingRowReplaceRecord() throws Exception {
    Column c1 = newColumn("testcol1", "value104");
    Column c2 = newColumn("testcol2", "value105");
    Column c3 = newColumn("testcol3", "value105");
    String rec = "record-100";
    RecordMutation rm = newRecordMutation(REPLACE_ENTIRE_RECORD, FAMILY, rec, c1, c2, c3);

    updateAndFetchRecord("row-100", rec, rm);
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

  @Test(expected = BlurException.class)
  public void testMutationUpdateRowMissingRecordReplaceColumns() throws Exception {
    Column c1 = newColumn("testcol4", "value999");
    Column c2 = newColumn("testcol5", "value9999");
    String rec = "record-1B";
    RecordMutation rm = newRecordMutation(REPLACE_COLUMNS, FAMILY, rec, c1, c2);

    updateAndFetchRecord("row-1", rec, rm);
  }

  @Test(expected = BlurException.class)
  public void testMutationUpdateMissingRowReplaceColumns() throws Exception {
    Column c1 = newColumn("testcol1", "value999");
    Column c2 = newColumn("testcol2", "value9999");
    String rec = "record-6";
    RecordMutation rm = newRecordMutation(REPLACE_COLUMNS, FAMILY, rec, c1, c2);

    updateAndFetchRecord("row-6", rec, rm);
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

  @Test(expected = BlurException.class)
  public void testMutationUpdateRowMissingRecordAppendColumns() throws Exception {
    Column c1 = newColumn("testcol1", "value999");
    Column c2 = newColumn("testcol2", "value9999");
    Column c3 = newColumn("testcol4", "hmm");
    String rec = "record-1B";
    RecordMutation rm = newRecordMutation(APPEND_COLUMN_VALUES, FAMILY, rec, c1, c2, c3);

    updateAndFetchRecord("row-1", rec, rm);
  }

  @Test(expected = BlurException.class)
  public void testMutationUpdateMissingRowAppendColumns() throws Exception {
    Column c1 = newColumn("testcol1", "value999");
    Column c2 = newColumn("testcol2", "value9999");
    String rec = "record-6";
    RecordMutation rm = newRecordMutation(APPEND_COLUMN_VALUES, FAMILY, rec, c1, c2);

    updateAndFetchRecord("row-6", rec, rm);
  }

  private Record updateAndFetchRecord(String rowId, String recordId, RecordMutation... recordMutations)
      throws Exception {
    RowMutation rowMutation = newRowMutation(UPDATE_ROW, TABLE, rowId, recordMutations);
    rowMutation.waitToBeVisible = true;
    indexManager.mutate(rowMutation);

    Selector selector = new Selector().setRowId(rowId).setRecordId(recordId);
    selector.setRecordOnly(true);
    FetchResult fetchResult = new FetchResult();
    indexManager.fetchRow(TABLE, selector, fetchResult);
    return (fetchResult.recordResult != null ? fetchResult.recordResult.record : null);
  }

}
