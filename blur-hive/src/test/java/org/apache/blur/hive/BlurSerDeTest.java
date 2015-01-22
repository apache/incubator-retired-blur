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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
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
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.GCWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlurSerDeTest {

  private static final String YYYYMMDD = "yyyyMMdd";
  private static final String TEST = "test";
  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_BlurSerDeTest"));
  private static MiniCluster miniCluster;
  private static boolean externalProcesses = true;

  @BeforeClass
  public static void startCluster() throws IOException {
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
  }

  @AfterClass
  public static void shutdownCluster() {
    miniCluster.shutdownBlurCluster();
  }

  @Before
  public void setup() throws BlurException, TException, IOException {
    String controllerConnectionStr = miniCluster.getControllerConnectionStr();
    Iface client = BlurClient.getClient(controllerConnectionStr);
    List<String> tableList = client.tableList();
    if (!tableList.contains(TEST)) {
      TableDescriptor tableDescriptor = new TableDescriptor();
      tableDescriptor.setName(TEST);
      tableDescriptor.setShardCount(1);
      tableDescriptor.setTableUri(miniCluster.getFileSystemUri().toString() + "/blur/tables/test");
      client.createTable(tableDescriptor);

      Map<String, String> props = new HashMap<String, String>();
      props.put("dateFormat", YYYYMMDD);

      client.addColumnDefinition(TEST, cd(false, "fam0", "string-col-single", "string"));
      client.addColumnDefinition(TEST, cd(false, "fam0", "text-col-single", "text"));
      client.addColumnDefinition(TEST, cd(false, "fam0", "stored-col-single", "stored"));
      client.addColumnDefinition(TEST, cd(false, "fam0", "double-col-single", "double"));
      client.addColumnDefinition(TEST, cd(false, "fam0", "float-col-single", "float"));
      client.addColumnDefinition(TEST, cd(false, "fam0", "long-col-single", "long"));
      client.addColumnDefinition(TEST, cd(false, "fam0", "int-col-single", "int"));
      client.addColumnDefinition(TEST, cd(false, "fam0", "date-col-single", "date", props));

      client.addColumnDefinition(TEST, cd(false, "fam0", "geo-col-single", "geo-pointvector"));

      client.addColumnDefinition(TEST, cd(true, "fam0", "string-col-multi", "string"));
      client.addColumnDefinition(TEST, cd(true, "fam0", "text-col-multi", "text"));
      client.addColumnDefinition(TEST, cd(true, "fam0", "stored-col-multi", "stored"));
      client.addColumnDefinition(TEST, cd(true, "fam0", "double-col-multi", "double"));
      client.addColumnDefinition(TEST, cd(true, "fam0", "float-col-multi", "float"));
      client.addColumnDefinition(TEST, cd(true, "fam0", "long-col-multi", "long"));
      client.addColumnDefinition(TEST, cd(true, "fam0", "int-col-multi", "int"));
      client.addColumnDefinition(TEST, cd(true, "fam0", "date-col-multi", "date", props));
    }
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
    tbl.put(BlurSerDe.FAMILY, "fam0");
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
}
