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
package org.apache.blur.mapreduce.lib.update;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.blur.MiniCluster;
import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DriverTest {

  private static Configuration conf = new Configuration();
  private static MiniCluster miniCluster;

  @BeforeClass
  public static void setupTest() throws Exception {
    setupJavaHome();
    File file = new File("./target/tmp/BlurInputFormatTest_tmp");
    String pathStr = file.getAbsoluteFile().toURI().toString();
    System.setProperty("test.build.data", pathStr + "/data");
    System.setProperty("hadoop.log.dir", pathStr + "/hadoop_log");
    miniCluster = new MiniCluster();
    miniCluster.startBlurCluster(pathStr + "/blur", 2, 2);
    miniCluster.startMrMiniCluster();
    conf = miniCluster.getMRConfiguration();

    BufferStore.initNewBuffer(128, 128 * 128);
  }

  public static void setupJavaHome() {
    String str = System.getenv("JAVA_HOME");
    if (str == null) {
      String property = System.getProperty("java.home");
      if (property != null) {
        throw new RuntimeException("JAVA_HOME not set should probably be [" + property + "].");
      }
      throw new RuntimeException("JAVA_HOME not set.");
    }
  }

  @AfterClass
  public static void teardown() throws IOException {
    if (miniCluster != null) {
      miniCluster.stopMrMiniCluster();
    }
    rm(new File("build"));
  }

  private static void rm(File file) {
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
  public void testDriverAddSingleRowWithSingleRecord() throws Exception {
    FileSystem fileSystem = miniCluster.getFileSystem();
    Path root = new Path(fileSystem.getUri() + "/");

    String tableName = "testDriverAddSingleRowWithSingleRecord";
    creatTable(tableName, new Path(root, "tables"), true);

    Driver driver = new Driver();
    driver.setConf(conf);

    String mrIncWorkingPathStr = new Path(root, "working").toString();
    generateData(mrIncWorkingPathStr);
    String outputPathStr = new Path(root, "output").toString();
    String blurZkConnection = miniCluster.getZkConnectionString();

    assertEquals(0, driver.run(new String[] { tableName, mrIncWorkingPathStr, outputPathStr, blurZkConnection, "1" }));

    Iface client = getClient();
    client.loadData(tableName, outputPathStr);

    waitUntilAllImportsAreCompleted(client, tableName);

    TableStats tableStats = client.tableStats(tableName);
    assertEquals(1, tableStats.getRowCount());
    assertEquals(1, tableStats.getRecordCount());
  }

  @Test
  public void testDriverAddSingleRecordToExistingRow() throws Exception {
    FileSystem fileSystem = miniCluster.getFileSystem();
    Path root = new Path(fileSystem.getUri() + "/");

    String tableName = "testDriverAddSingleRecordToExistingRow";
    Iface client = getClient();
    creatTable(tableName, new Path(root, "tables"), true);
    addRow(client, tableName, "row1", "record1", "value1");

    Driver driver = new Driver();
    driver.setConf(conf);

    String mrIncWorkingPathStr = new Path(root, "working").toString();
    generateData(mrIncWorkingPathStr);
    String outputPathStr = new Path(root, "output").toString();
    String blurZkConnection = miniCluster.getZkConnectionString();

    assertEquals(0, driver.run(new String[] { tableName, mrIncWorkingPathStr, outputPathStr, blurZkConnection, "1" }));

    client.loadData(tableName, outputPathStr);

    waitUntilAllImportsAreCompleted(client, tableName);

    TableStats tableStats = client.tableStats(tableName);
    assertEquals(1, tableStats.getRowCount());
    assertEquals(2, tableStats.getRecordCount());
  }

  @Test
  public void testDriverUpdateRecordToExistingRow() throws Exception {
    FileSystem fileSystem = miniCluster.getFileSystem();
    Path root = new Path(fileSystem.getUri() + "/");

    String tableName = "testDriverUpdateRecordToExistingRow";
    Iface client = getClient();
    creatTable(tableName, new Path(root, "tables"), true);
    String rowId = "row1";
    String recordId = "record1";
    addRow(client, tableName, rowId, recordId, "value1");

    Driver driver = new Driver();
    driver.setConf(conf);

    String mrIncWorkingPathStr = new Path(root, "working").toString();
    generateData(mrIncWorkingPathStr, rowId, recordId, "value2");
    String outputPathStr = new Path(root, "output").toString();
    String blurZkConnection = miniCluster.getZkConnectionString();

    assertEquals(0, driver.run(new String[] { tableName, mrIncWorkingPathStr, outputPathStr, blurZkConnection, "1" }));
    {
      Selector selector = new Selector();
      selector.setRowId(rowId);
      FetchResult fetchRow = client.fetchRow(tableName, selector);
      Row row = fetchRow.getRowResult().getRow();
      assertEquals(rowId, row.getId());
      List<Record> records = row.getRecords();
      assertEquals(1, records.size());
      Record record = records.get(0);
      assertEquals(recordId, record.getRecordId());
      List<Column> columns = record.getColumns();
      assertEquals(1, columns.size());
      Column column = columns.get(0);
      assertEquals("col0", column.getName());
      assertEquals("value1", column.getValue());
    }

    client.loadData(tableName, outputPathStr);

    waitUntilAllImportsAreCompleted(client, tableName);

    TableStats tableStats = client.tableStats(tableName);
    assertEquals(1, tableStats.getRowCount());
    assertEquals(1, tableStats.getRecordCount());

    {
      Selector selector = new Selector();
      selector.setRowId(rowId);
      FetchResult fetchRow = client.fetchRow(tableName, selector);
      Row row = fetchRow.getRowResult().getRow();
      assertEquals(rowId, row.getId());
      List<Record> records = row.getRecords();
      assertEquals(1, records.size());
      Record record = records.get(0);
      assertEquals(recordId, record.getRecordId());
      List<Column> columns = record.getColumns();
      assertEquals(1, columns.size());
      Column column = columns.get(0);
      assertEquals("col0", column.getName());
      assertEquals("value2", column.getValue());
    }
  }

  @Test
  public void testBulkTableUpdateCommandUpdateRecordToExistingRow() throws Exception {
    FileSystem fileSystem = miniCluster.getFileSystem();
    Path root = new Path(fileSystem.getUri() + "/");

    String tableName = "testBulkTableUpdateCommandUpdateRecordToExistingRow";
    Iface client = getClient();
    Path mrIncWorkingPath = new Path(new Path(root, "working"), tableName);
    creatTable(tableName, new Path(root, "tables"), true, mrIncWorkingPath.toString());
    String rowId = "row1";
    String recordId = "record1";
    addRow(client, tableName, rowId, recordId, "value1");

    generateData(mrIncWorkingPath.toString(), rowId, recordId, "value2");

    {
      Selector selector = new Selector();
      selector.setRowId(rowId);
      FetchResult fetchRow = client.fetchRow(tableName, selector);
      Row row = fetchRow.getRowResult().getRow();
      assertEquals(rowId, row.getId());
      List<Record> records = row.getRecords();
      assertEquals(1, records.size());
      Record record = records.get(0);
      assertEquals(recordId, record.getRecordId());
      List<Column> columns = record.getColumns();
      assertEquals(1, columns.size());
      Column column = columns.get(0);
      assertEquals("col0", column.getName());
      assertEquals("value1", column.getValue());
    }

    BulkTableUpdateCommand bulkTableUpdateCommand = new BulkTableUpdateCommand();
    bulkTableUpdateCommand.setAutoLoad(true);
    bulkTableUpdateCommand.setTable(tableName);
    bulkTableUpdateCommand.setWaitForDataBeVisible(true);
    bulkTableUpdateCommand.addExtraConfig(conf);
    assertEquals(0, (int) bulkTableUpdateCommand.run(getClient()));

    TableStats tableStats = client.tableStats(tableName);
    assertEquals(1, tableStats.getRowCount());
    assertEquals(1, tableStats.getRecordCount());

    {
      Selector selector = new Selector();
      selector.setRowId(rowId);
      FetchResult fetchRow = client.fetchRow(tableName, selector);
      Row row = fetchRow.getRowResult().getRow();
      assertEquals(rowId, row.getId());
      List<Record> records = row.getRecords();
      assertEquals(1, records.size());
      Record record = records.get(0);
      assertEquals(recordId, record.getRecordId());
      List<Column> columns = record.getColumns();
      assertEquals(1, columns.size());
      Column column = columns.get(0);
      assertEquals("col0", column.getName());
      assertEquals("value2", column.getValue());
    }
  }

  private void addRow(Iface client, String tableName, String rowId, String recordId, String value)
      throws BlurException, TException {
    List<RecordMutation> recordMutations = new ArrayList<RecordMutation>();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("col0", value));
    Record record = new Record(recordId, "fam0", columns);
    recordMutations.add(new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, record));
    RowMutation rowMutation = new RowMutation(tableName, rowId, RowMutationType.REPLACE_ROW, recordMutations);
    client.mutate(rowMutation);
  }

  private void waitUntilAllImportsAreCompleted(Iface client, String tableName) throws BlurException, TException,
      InterruptedException {
    while (true) {
      Thread.sleep(1000);
      TableStats tableStats = client.tableStats(tableName);
      if (tableStats.getSegmentImportInProgressCount() == 0 && tableStats.getSegmentImportPendingCount() == 0) {
        return;
      }
    }
  }

  private void generateData(String mrIncWorkingPathStr, String rowId, String recordId, String value) throws IOException {
    Path path = new Path(new Path(mrIncWorkingPathStr), "new");
    Writer writer = new SequenceFile.Writer(miniCluster.getFileSystem(), conf, new Path(path, UUID.randomUUID()
        .toString()), Text.class, BlurRecord.class);
    BlurRecord blurRecord = new BlurRecord();
    blurRecord.setRowId(rowId);
    blurRecord.setRecordId(recordId);
    blurRecord.setFamily("fam0");
    blurRecord.addColumn("col0", value);
    writer.append(new Text(rowId), blurRecord);
    writer.close();
  }

  private void generateData(String mrIncWorkingPathStr) throws IOException {
    generateData(mrIncWorkingPathStr, "row1", "record-" + System.currentTimeMillis(), "val0");
  }

  private void creatTable(String tableName, Path tables, boolean fastDisable) throws BlurException, TException {
    creatTable(tableName, tables, fastDisable, null);
  }

  private void creatTable(String tableName, Path tables, boolean fastDisable, String workingPath) throws BlurException,
      TException {
    Path tablePath = new Path(tables, tableName);
    Iface client = getClient();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setTableUri(tablePath.toString());
    tableDescriptor.setName(tableName);
    tableDescriptor.setShardCount(2);
    tableDescriptor.putToTableProperties(BlurConstants.BLUR_TABLE_DISABLE_FAST_DIR, Boolean.toString(fastDisable));
    if (workingPath != null) {
      tableDescriptor.putToTableProperties(BlurConstants.BLUR_BULK_UPDATE_WORKING_PATH, workingPath);
    }
    client.createTable(tableDescriptor);

    ColumnDefinition colDef = new ColumnDefinition();
    colDef.setFamily("fam0");
    colDef.setColumnName("col0");
    colDef.setFieldType("string");
    client.addColumnDefinition(tableName, colDef);
  }

  private Iface getClient() {
    return BlurClient.getClientFromZooKeeperConnectionStr(miniCluster.getZkConnectionString());
  }
}
