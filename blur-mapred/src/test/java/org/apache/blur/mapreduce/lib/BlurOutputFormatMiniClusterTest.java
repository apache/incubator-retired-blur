package org.apache.blur.mapreduce.lib;

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
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.blur.MiniCluster;
import org.apache.blur.server.TableContext;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.utils.GCWatcher;
import org.apache.blur.utils.JavaHome;
import org.apache.blur.utils.ShardUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlurOutputFormatMiniClusterTest {

  private static Configuration conf = new Configuration();
  private static FileSystem fileSystem;
  private static Path TEST_ROOT_DIR;
  private static MiniCluster miniCluster;
  private Path inDir = new Path(TEST_ROOT_DIR + "/in");
  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir",
      "./target/tmp_BlurOutputFormatMiniClusterTest"));

  @BeforeClass
  public static void setupTest() throws Exception {
    GCWatcher.init(0.60);
    JavaHome.checkJavaHome();
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
    miniCluster.startBlurCluster(new File(testDirectory, "cluster").getAbsolutePath(), 2, 3, true, false);

    TEST_ROOT_DIR = new Path(miniCluster.getFileSystemUri().toString() + "/blur_test");
    System.setProperty("hadoop.log.dir", "./target/BlurOutputFormatTest/hadoop_log");
    try {
      fileSystem = TEST_ROOT_DIR.getFileSystem(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }

    FileSystem.setDefaultUri(conf, miniCluster.getFileSystemUri());

    miniCluster.startMrMiniCluster();
    conf = miniCluster.getMRConfiguration();

    BufferStore.initNewBuffer(128, 128 * 128);
  }

  @AfterClass
  public static void teardown() throws IOException {
    if (miniCluster != null) {
      miniCluster.stopMrMiniCluster();
    }
    miniCluster.shutdownBlurCluster();
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

  @Before
  public void setup() {
    TableContext.clear();
  }

  @Test
  public void testBlurOutputFormat() throws IOException, InterruptedException, ClassNotFoundException, BlurException,
      TException {
    fileSystem.delete(inDir, true);
    String tableName = "testBlurOutputFormat";
    writeRecordsFile("in/part1", 1, 1, 1, 1, "cf1");
    writeRecordsFile("in/part2", 1, 1, 2, 1, "cf1");

    Job job = Job.getInstance(conf, "blur index");
    job.setJarByClass(BlurOutputFormatMiniClusterTest.class);
    job.setMapperClass(CsvBlurMapper.class);
    job.setInputFormatClass(TextInputFormat.class);

    FileInputFormat.addInputPath(job, new Path(TEST_ROOT_DIR + "/in"));
    String tableUri = new Path(TEST_ROOT_DIR + "/blur/" + tableName).makeQualified(fileSystem.getUri(),
        fileSystem.getWorkingDirectory()).toString();
    CsvBlurMapper.addColumns(job, "cf1", "col");

    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setShardCount(1);
    tableDescriptor.setTableUri(tableUri);
    tableDescriptor.setName(tableName);

    Iface client = getClient();
    client.createTable(tableDescriptor);

    BlurOutputFormat.setupJob(job, tableDescriptor);
    Path output = new Path(TEST_ROOT_DIR + "/out");
    BlurOutputFormat.setOutputPath(job, output);

    Path tablePath = new Path(tableUri);
    Path shardPath = new Path(tablePath, ShardUtil.getShardName(0));
    FileStatus[] listStatus = fileSystem.listStatus(shardPath);
    
    System.out.println("======" + listStatus.length);
    for (FileStatus fileStatus : listStatus) {
      System.out.println(fileStatus.getPath());
    }
    assertEquals(4, listStatus.length);

    assertTrue(job.waitForCompletion(true));
    Counters ctrs = job.getCounters();
    System.out.println("Counters: " + ctrs);

    client.loadData(tableName, output.toString());

    while (true) {
      TableStats tableStats = client.tableStats(tableName);
      System.out.println(tableStats);
      if (tableStats.getRowCount() > 0) {
        break;
      }
      Thread.sleep(100);
    }

    assertTrue(fileSystem.exists(tablePath));
    assertFalse(fileSystem.isFile(tablePath));

    FileStatus[] listStatusAfter = fileSystem.listStatus(shardPath);

    assertEquals(12, listStatusAfter.length);

  }

  private Iface getClient() {
    return BlurClient.getClient(miniCluster.getControllerConnectionStr());
  }

  public static String readFile(String name) throws IOException {
    DataInputStream f = fileSystem.open(new Path(TEST_ROOT_DIR + "/" + name));
    BufferedReader b = new BufferedReader(new InputStreamReader(f));
    StringBuilder result = new StringBuilder();
    String line = b.readLine();
    while (line != null) {
      result.append(line);
      result.append('\n');
      line = b.readLine();
    }
    b.close();
    return result.toString();
  }

  private Path writeRecordsFile(String name, int starintgRowId, int numberOfRows, int startRecordId,
      int numberOfRecords, String family) throws IOException {
    // "1,1,cf1,val1"
    Path file = new Path(TEST_ROOT_DIR + "/" + name);
    fileSystem.delete(file, false);
    DataOutputStream f = fileSystem.create(file);
    PrintWriter writer = new PrintWriter(f);
    for (int row = 0; row < numberOfRows; row++) {
      for (int record = 0; record < numberOfRecords; record++) {
        writer.println(getRecord(row + starintgRowId, record + startRecordId, family));
      }
    }
    writer.close();
    return file;
  }

  private String getRecord(int rowId, int recordId, String family) {
    return rowId + "," + recordId + "," + family + ",valuetoindex";
  }
}
