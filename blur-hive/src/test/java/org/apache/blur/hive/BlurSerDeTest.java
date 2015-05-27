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
package org.apache.blur.hive;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.blur.MiniCluster;
import org.apache.blur.mapreduce.lib.BlurColumn;
import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.GCWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.hive.service.server.HiveServer2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlurSerDeTest {

  public static final File WAREHOUSE = new File("./target/tmp/warehouse");
  public static final String COLUMN_SEP = new String(new char[] { 1 });
  public static final String ITEM_SEP = new String(new char[] { 2 });
  public static final File DERBY_FILE = new File("derby.log");
  public static final File METASTORE_DB_FILE = new File("metastore_db");

  private static final String FAM = "fam0";
  private static final String YYYYMMDD = "yyyyMMdd";
  private static final String YYYY_MM_DD = "yyyy-MM-dd";
  private static final String TEST = "test";
  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_BlurSerDeTest"));
  private static MiniCluster miniCluster;
  private static boolean externalProcesses = false;

  @BeforeClass
  public static void startCluster() throws IOException {
    System.setProperty("hadoop.log.dir", "./target/tmp_BlurSerDeTest_hadoop_log");
    GCWatcher.init(0.60);
    LocalFileSystem localFS = FileSystem.getLocal(new Configuration());
    File testDirectory = new File(TMPDIR, "blur-SerDe-test").getAbsoluteFile();
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
    miniCluster.startMrMiniCluster();
  }

  @AfterClass
  public static void shutdownCluster() throws IOException {
    miniCluster.stopMrMiniCluster();
    miniCluster.shutdownBlurCluster();
  }

  private String _mrWorkingPath;

  @Before
  public void setup() throws BlurException, TException, IOException {
    _mrWorkingPath = miniCluster.getFileSystemUri().toString() + "/mrworkingpath";
    String controllerConnectionStr = miniCluster.getControllerConnectionStr();
    Iface client = BlurClient.getClient(controllerConnectionStr);
    List<String> tableList = client.tableList();
    if (!tableList.contains(TEST)) {
      TableDescriptor tableDescriptor = new TableDescriptor();
      tableDescriptor.setName(TEST);
      tableDescriptor.setShardCount(1);
      tableDescriptor.setTableUri(miniCluster.getFileSystemUri().toString() + "/blur/tables/test");
      tableDescriptor.putToTableProperties(BlurConstants.BLUR_BULK_UPDATE_WORKING_PATH, _mrWorkingPath);

      client.createTable(tableDescriptor);

      Map<String, String> props = new HashMap<String, String>();
      props.put("dateFormat", YYYYMMDD);

      client.addColumnDefinition(TEST, cd(false, FAM, "string-col-single", "string"));
      client.addColumnDefinition(TEST, cd(false, FAM, "text-col-single", "text"));
      client.addColumnDefinition(TEST, cd(false, FAM, "stored-col-single", "stored"));
      client.addColumnDefinition(TEST, cd(false, FAM, "double-col-single", "double"));
      client.addColumnDefinition(TEST, cd(false, FAM, "float-col-single", "float"));
      client.addColumnDefinition(TEST, cd(false, FAM, "long-col-single", "long"));
      client.addColumnDefinition(TEST, cd(false, FAM, "int-col-single", "int"));
      client.addColumnDefinition(TEST, cd(false, FAM, "date-col-single", "date", props));

      client.addColumnDefinition(TEST, cd(false, FAM, "geo-col-single", "geo-pointvector"));

      client.addColumnDefinition(TEST, cd(true, FAM, "string-col-multi", "string"));
      client.addColumnDefinition(TEST, cd(true, FAM, "text-col-multi", "text"));
      client.addColumnDefinition(TEST, cd(true, FAM, "stored-col-multi", "stored"));
      client.addColumnDefinition(TEST, cd(true, FAM, "double-col-multi", "double"));
      client.addColumnDefinition(TEST, cd(true, FAM, "float-col-multi", "float"));
      client.addColumnDefinition(TEST, cd(true, FAM, "long-col-multi", "long"));
      client.addColumnDefinition(TEST, cd(true, FAM, "int-col-multi", "int"));
      client.addColumnDefinition(TEST, cd(true, FAM, "date-col-multi", "date", props));
    }
    rmr(WAREHOUSE);
    rmr(METASTORE_DB_FILE);
    rmr(DERBY_FILE);
  }

  @After
  public void teardown() {
    rmr(WAREHOUSE);
    rmr(METASTORE_DB_FILE);
    rmr(DERBY_FILE);
  }

  public static void rmr(File file) {
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

  private ColumnDefinition cd(boolean multiValue, String family, String columnName, String type) {
    return cd(multiValue, family, columnName, type, null);
  }

  private ColumnDefinition cd(boolean multiValue, String family, String columnName, String type,
      Map<String, String> props) {
    ColumnDefinition columnDefinition = new ColumnDefinition(family, columnName, null, false, type, props, false);
    columnDefinition.setMultiValueField(multiValue);
    return columnDefinition;
  }

  @Test
  public void test1() throws SerDeException {
    long now = System.currentTimeMillis();
    Date date = new Date(now);
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(YYYYMMDD);
    BlurSerDe blurSerDe = new BlurSerDe();

    Configuration conf = new Configuration();
    Properties tbl = new Properties();
    tbl.put(BlurSerDe.TABLE, TEST);
    tbl.put(BlurSerDe.FAMILY, FAM);
    tbl.put(BlurSerDe.ZK, miniCluster.getZkConnectionString());

    blurSerDe.initialize(conf, tbl);

    ObjectInspector objectInspector = blurSerDe.getObjectInspector();
    Object[] row = new Object[19];
    int c = 0;
    row[c++] = "rowid";
    row[c++] = "recordid";
    row[c++] = new Object[] { date, date };
    row[c++] = date;
    row[c++] = new Object[] { 1234.5678, 4321.5678 };
    row[c++] = 1234.5678;
    row[c++] = new Object[] { 1234.567f, 4321.567f };
    row[c++] = 1234.567f;
    row[c++] = new Object[] { 1.0f, 2.0f };
    row[c++] = new Object[] { 12345678, 87654321 };
    row[c++] = 12345678;
    row[c++] = new Object[] { 12345678l, 87654321l };
    row[c++] = 12345678l;
    row[c++] = new Object[] { "stored input1", "stored input2" };
    row[c++] = "stored input";
    row[c++] = new Object[] { "string input1", "string input2" };
    row[c++] = "string input";
    row[c++] = new Object[] { "text input1", "text input2" };
    row[c++] = "text input";

    BlurRecord blurRecord = (BlurRecord) blurSerDe.serialize(row, objectInspector);
    assertEquals("rowid", blurRecord.getRowId());
    assertEquals("recordid", blurRecord.getRecordId());

    Map<String, List<String>> columns = toMap(blurRecord.getColumns());

    assertEquals(list("string input"), columns.get("string-col-single"));
    assertEquals(list("string input1", "string input2"), columns.get("string-col-multi"));

    assertEquals(list("text input"), columns.get("text-col-single"));
    assertEquals(list("text input1", "text input2"), columns.get("text-col-multi"));

    assertEquals(list("stored input"), columns.get("stored-col-single"));
    assertEquals(list("stored input1", "stored input2"), columns.get("stored-col-multi"));

    assertEquals(list("1234.5678"), columns.get("double-col-single"));
    assertEquals(list("1234.5678", "4321.5678"), columns.get("double-col-multi"));

    assertEquals(list("1234.567"), columns.get("float-col-single"));
    assertEquals(list("1234.567", "4321.567"), columns.get("float-col-multi"));

    assertEquals(list("12345678"), columns.get("long-col-single"));
    assertEquals(list("12345678", "87654321"), columns.get("long-col-multi"));

    assertEquals(list("12345678"), columns.get("int-col-single"));
    assertEquals(list("12345678", "87654321"), columns.get("int-col-multi"));

    assertEquals(list(simpleDateFormat.format(date)), columns.get("date-col-single"));
    assertEquals(list(simpleDateFormat.format(date), simpleDateFormat.format(date)), columns.get("date-col-multi"));

    assertEquals(list("1.0,2.0"), columns.get("geo-col-single"));
  }

  @Test
  public void test2() throws SQLException, ClassNotFoundException, IOException, BlurException, TException,
      InterruptedException {
    int totalRecords = runLoad(true);
    Iface client = BlurClient.getClientFromZooKeeperConnectionStr(miniCluster.getZkConnectionString());
    BlurQuery blurQuery = new BlurQuery();
    Query query = new Query();
    query.setQuery("*");
    blurQuery.setQuery(query);
    BlurResults results = client.query(TEST, blurQuery);
    assertEquals(totalRecords, results.getTotalResults());
  }

  @Test
  public void test3() throws Exception {
    int totalRecords = runLoad(false);
    Iface client = BlurClient.getClientFromZooKeeperConnectionStr(miniCluster.getZkConnectionString());
    BlurQuery blurQuery = new BlurQuery();
    Query query = new Query();
    query.setQuery("*");
    blurQuery.setQuery(query);
    BlurResults results = client.query(TEST, blurQuery);
    assertEquals(totalRecords, results.getTotalResults());
  }

  private int runLoad(boolean disableMrUpdate) throws IOException, InterruptedException, ClassNotFoundException,
      SQLException {

    Configuration configuration = miniCluster.getMRConfiguration();
    HiveConf hiveConf = new HiveConf(configuration, getClass());
    hiveConf.set("hive.server2.thrift.port", "0");
    HiveServer2 hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();

    int port = waitForStartupAndGetPort(hiveServer2);

    Class.forName(HiveDriver.class.getName());
    String userName = UserGroupInformation.getCurrentUser().getShortUserName();
    Connection connection = DriverManager.getConnection("jdbc:hive2://localhost:" + port, userName, "");

    run(connection, "set blur.mr.update.disabled=" + disableMrUpdate);
    run(connection, "set hive.metastore.warehouse.dir=" + WAREHOUSE.toURI().toString());
    run(connection, "create database if not exists testdb");
    run(connection, "use testdb");

    run(connection, "CREATE TABLE if not exists testtable ROW FORMAT SERDE 'org.apache.blur.hive.BlurSerDe' "
        + "WITH SERDEPROPERTIES ( 'blur.zookeeper.connection'='" + miniCluster.getZkConnectionString() + "', "
        + "'blur.table'='" + TEST + "', 'blur.family'='" + FAM + "' ) "
        + "STORED BY 'org.apache.blur.hive.BlurHiveStorageHandler'");

    run(connection, "desc testtable");

    String createLoadTable = buildCreateLoadTable(connection);
    run(connection, createLoadTable);
    File dbDir = new File(WAREHOUSE, "testdb.db");
    File tableDir = new File(dbDir, "loadtable");
    int totalRecords = 100;
    generateData(tableDir, totalRecords);

    run(connection, "select * from loadtable");
    run(connection, "set " + BlurSerDe.BLUR_BLOCKING_APPLY + "=true");
    run(connection, "insert into table testtable select * from loadtable");
    connection.close();
    hiveServer2.stop();
    return totalRecords;
  }

  private void generateData(File file, int totalRecords) throws IOException {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(YYYY_MM_DD);
    file.mkdirs();
    PrintWriter print = new PrintWriter(new File(file, "data"));
    Date date = new Date(System.currentTimeMillis());
    for (int i = 0; i < totalRecords; i++) {
      // rowid
      print.print("rowid" + i);
      print.print(COLUMN_SEP);
      // recordid
      print.print("recordid" + i);
      print.print(COLUMN_SEP);
      {
        // date_col_multi
        print.print(simpleDateFormat.format(date));
        print.print(ITEM_SEP);
        print.print(simpleDateFormat.format(date));
      }
      print.print(COLUMN_SEP);
      // date_col_single
      print.print(simpleDateFormat.format(date));
      print.print(COLUMN_SEP);
      {
        // double_col_multi
        print.print("1.0");
        print.print(ITEM_SEP);
        print.print("2.0");
      }
      print.print(COLUMN_SEP);
      // double_col_single
      print.print("3.0");
      print.print(COLUMN_SEP);

      {
        // float_col_multi
        print.print("4.0");
        print.print(ITEM_SEP);
        print.print("5.0");
      }
      print.print(COLUMN_SEP);
      // float_col_single
      print.print("6.0");
      print.print(COLUMN_SEP);

      // geo_col_single
      print.print("10.0");
      print.print(ITEM_SEP);
      print.print("10.0");
      print.print(COLUMN_SEP);

      {
        // int_col_multi
        print.print("1");
        print.print(ITEM_SEP);
        print.print("2");
      }
      print.print(COLUMN_SEP);
      // int_col_single
      print.print("3");
      print.print(COLUMN_SEP);

      {
        // long_col_multi
        print.print("4");
        print.print(ITEM_SEP);
        print.print("5");
      }
      print.print(COLUMN_SEP);
      // long_col_single
      print.print("6");
      print.print(COLUMN_SEP);

      {
        // stored_col_multi
        print.print("stored_1");
        print.print(ITEM_SEP);
        print.print("stored_2");
      }
      print.print(COLUMN_SEP);
      // stored_col_single
      print.print("stored_3");
      print.print(COLUMN_SEP);

      {
        // string_col_multi
        print.print("string_1");
        print.print(ITEM_SEP);
        print.print("string_2");
      }
      print.print(COLUMN_SEP);
      // string_col_single
      print.print("string_3");
      print.print(COLUMN_SEP);

      {
        // text_col_multi
        print.print("text_1");
        print.print(ITEM_SEP);
        print.print("text_2");
      }
      print.print(COLUMN_SEP);
      // text_col_single
      print.print("text_3");
      print.println();
    }
    print.close();

  }

  private String buildCreateLoadTable(Connection connection) throws SQLException {
    StringBuilder builder = new StringBuilder("create TABLE if not exists loadtable (");
    Statement statement = connection.createStatement();
    if (statement.execute("desc testtable")) {
      ResultSet resultSet = statement.getResultSet();
      boolean first = true;
      while (resultSet.next()) {
        if (!first) {
          builder.append(", ");
        }
        Object name = resultSet.getObject(1);
        Object type = resultSet.getObject(2);
        builder.append(name.toString());
        builder.append(' ');
        builder.append(type.toString());
        first = false;
      }
      builder.append(")");
      return builder.toString();
    }
    throw new RuntimeException("Can't build create table script.");
  }

  public static void run(Connection connection, String sql) throws SQLException {
    System.out.println("Running:" + sql);
    Statement statement = connection.createStatement();
    if (statement.execute(sql)) {
      ResultSet resultSet = statement.getResultSet();
      while (resultSet.next()) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          System.out.print(resultSet.getObject(i) + "\t");
        }
        System.out.println();
      }
    }
    statement.close();
  }

  private List<String> list(String... sarray) {
    List<String> list = new ArrayList<String>();
    for (String s : sarray) {
      list.add(s);
    }
    return list;
  }

  private Map<String, List<String>> toMap(List<BlurColumn> columns) {
    Map<String, List<String>> map = new HashMap<String, List<String>>();
    for (BlurColumn blurColumn : columns) {
      String name = blurColumn.getName();
      List<String> list = map.get(name);
      if (list == null) {
        map.put(name, list = new ArrayList<String>());
      }
      list.add(blurColumn.getValue());
    }
    return map;
  }

  @SuppressWarnings("resource")
  private int waitForStartupAndGetPort(HiveServer2 hiveServer2) throws InterruptedException {
    while (true) {
      // thriftCLIService->server->serverTransport_->serverSocket_
      Thread.sleep(100);
      Object o1 = getObject(hiveServer2, "thriftCLIService");
      if (o1 == null) {
        continue;
      }
      Object o2 = getObject(o1, "server");
      if (o2 == null) {
        continue;
      }
      Object o3 = getObject(o2, "serverTransport_");
      if (o3 == null) {
        continue;
      }
      Object o4 = getObject(o3, "serverSocket_");
      if (o4 == null) {
        continue;
      }
      ServerSocket socket = (ServerSocket) o4;
      return socket.getLocalPort();
    }
  }

  private Object getObject(Object o, String field) {
    return getObject(o, field, o.getClass());
  }

  private Object getObject(Object o, String field, Class<? extends Object> clazz) {
    try {
      Field declaredField = clazz.getDeclaredField(field);
      return getObject(o, declaredField);
    } catch (NoSuchFieldException e) {
      return getObject(o, field, clazz.getSuperclass());
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private Object getObject(Object o, Field field) {
    field.setAccessible(true);
    try {
      return field.get(o);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
