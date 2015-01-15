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
      client.addColumnDefinition(TEST, new ColumnDefinition("fam0", "string-col", null, false, "string", null, false));
      client.addColumnDefinition(TEST, new ColumnDefinition("fam0", "text-col", null, false, "text", null, false));
      client.addColumnDefinition(TEST, new ColumnDefinition("fam0", "stored-col", null, false, "stored", null, false));
      client.addColumnDefinition(TEST, new ColumnDefinition("fam0", "double-col", null, false, "double", null, false));
      client.addColumnDefinition(TEST, new ColumnDefinition("fam0", "float-col", null, false, "float", null, false));
      client.addColumnDefinition(TEST, new ColumnDefinition("fam0", "long-col", null, false, "long", null, false));
      client.addColumnDefinition(TEST, new ColumnDefinition("fam0", "int-col", null, false, "int", null, false));
      Map<String, String> props = new HashMap<String, String>();
      props.put("dateFormat", YYYYMMDD);
      client.addColumnDefinition(TEST, new ColumnDefinition("fam0", "date-col", null, false, "date", props, false));
      client.addColumnDefinition(TEST, new ColumnDefinition("fam0", "geo-col", null, false, "geo-pointvector", null,
          false));

    }
  }

  @Test
  public void test1() throws SerDeException {
    long now = System.currentTimeMillis();
    Date date = new Date(now);
    BlurSerDe blurSerDe = new BlurSerDe();

    Configuration conf = new Configuration();
    Properties tbl = new Properties();
    tbl.put(BlurSerDe.TABLE, TEST);
    tbl.put(BlurSerDe.FAMILY, "fam0");
    tbl.put(BlurSerDe.ZK, miniCluster.getZkConnectionString());

    blurSerDe.initialize(conf, tbl);

    ObjectInspector objectInspector = blurSerDe.getObjectInspector();
    Object[] row = new Object[11];
    int c = 0;
    row[c++] = "rowid";
    row[c++] = "recordid";
    row[c++] = date;
    row[c++] = 1234.5678;
    row[c++] = 1234.567f;
    row[c++] = new Object[] { 1.0f, 2.0 };
    row[c++] = 12345678;
    row[c++] = 12345678l;
    row[c++] = "stored input";
    row[c++] = "string input";
    row[c++] = "text input";

    BlurRecord blurRecord = (BlurRecord) blurSerDe.serialize(row, objectInspector);
    assertEquals("rowid", blurRecord.getRowId());
    assertEquals("recordid", blurRecord.getRecordId());

    Map<String, String> columns = toMap(blurRecord.getColumns());
    assertEquals("string input", columns.get("string-col"));
    assertEquals("text input", columns.get("text-col"));
    assertEquals("stored input", columns.get("stored-col"));
    assertEquals("1234.5678", columns.get("double-col"));
    assertEquals("1234.567", columns.get("float-col"));
    assertEquals("12345678", columns.get("long-col"));
    assertEquals("12345678", columns.get("int-col"));
    assertEquals("1.0,2.0", columns.get("geo-col"));
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(YYYYMMDD);
    assertEquals(simpleDateFormat.format(date), columns.get("date-col"));
  }

  private Map<String, String> toMap(List<BlurColumn> columns) {
    Map<String, String> map = new HashMap<String, String>();
    for (BlurColumn blurColumn : columns) {
      map.put(blurColumn.getName(), blurColumn.getValue());
    }
    return map;
  }

}
