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
package org.apache.blur.mapreduce.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.blur.MiniCluster;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlurInputFormatTest {

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
  public void testBlurInputFormatFastDisabledNoFileCache() throws IOException, BlurException, TException,
      ClassNotFoundException, InterruptedException {
    String tableName = "testBlurInputFormatFastDisabledNoFileCache";
    runTest(tableName, true, null);
  }

  @Test
  public void testBlurInputFormatFastEnabledNoFileCache() throws IOException, BlurException, TException,
      ClassNotFoundException, InterruptedException {
    String tableName = "testBlurInputFormatFastEnabledNoFileCache";
    runTest(tableName, false, null);
  }

  @Test
  public void testBlurInputFormatFastDisabledFileCache() throws IOException, BlurException, TException,
      ClassNotFoundException, InterruptedException {
    String tableName = "testBlurInputFormatFastDisabledFileCache";
    Path fileCache = new Path(miniCluster.getFileSystemUri() + "/filecache");
    runTest(tableName, true, fileCache);
    FileSystem fileSystem = miniCluster.getFileSystem();
    // @TODO write some assertions.
    // RemoteIterator<LocatedFileStatus> listFiles =
    // fileSystem.listFiles(fileCache, true);
    // while (listFiles.hasNext()) {
    // LocatedFileStatus locatedFileStatus = listFiles.next();
    // System.out.println(locatedFileStatus.getPath());
    // }
  }

  @Test
  public void testBlurInputFormatFastEnabledFileCache() throws IOException, BlurException, TException,
      ClassNotFoundException, InterruptedException {
    String tableName = "testBlurInputFormatFastEnabledFileCache";
    Path fileCache = new Path(miniCluster.getFileSystemUri() + "/filecache");
    runTest(tableName, false, fileCache);
    FileSystem fileSystem = miniCluster.getFileSystem();
    // @TODO write some assertions.
    // RemoteIterator<LocatedFileStatus> listFiles =
    // fileSystem.listFiles(fileCache, true);
    // while (listFiles.hasNext()) {
    // LocatedFileStatus locatedFileStatus = listFiles.next();
    // System.out.println(locatedFileStatus.getPath());
    // }
  }

  private void runTest(String tableName, boolean disableFast, Path fileCache) throws IOException, BlurException,
      TException, InterruptedException, ClassNotFoundException {
    FileSystem fileSystem = miniCluster.getFileSystem();
    Path root = new Path(fileSystem.getUri() + "/");

    creatTable(tableName, new Path(root, "tables"), disableFast);
    loadTable(tableName, 100, 100);

    Iface client = getClient();

    TableDescriptor tableDescriptor = client.describe(tableName);

    Job job = Job.getInstance(conf, "Read Data");
    job.setJarByClass(BlurInputFormatTest.class);
    job.setMapperClass(TestMapper.class);
    job.setInputFormatClass(BlurInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TableBlurRecord.class);

    Path output = new Path(new Path(root, "output"), tableName);

    String snapshot = UUID.randomUUID().toString();
    client.createSnapshot(tableName, snapshot);

    if (fileCache != null) {
      BlurInputFormat.setLocalCachePath(job, fileCache);
    }

    BlurInputFormat.addTable(job, tableDescriptor, snapshot);
    FileOutputFormat.setOutputPath(job, output);

    try {
      assertTrue(job.waitForCompletion(true));
    } finally {
      client.removeSnapshot(tableName, snapshot);
    }

    final Map<Text, TableBlurRecord> results = new TreeMap<Text, TableBlurRecord>();
    walkOutput(output, conf, new ResultReader() {
      @Override
      public void read(Text rowId, TableBlurRecord tableBlurRecord) {
        results.put(new Text(rowId), new TableBlurRecord(tableBlurRecord));
      }
    });
    int rowId = 100;
    for (Entry<Text, TableBlurRecord> e : results.entrySet()) {
      Text r = e.getKey();
      assertEquals(new Text("row-" + rowId), r);
      BlurRecord blurRecord = new BlurRecord();
      blurRecord.setRowId("row-" + rowId);
      blurRecord.setRecordId("record-" + rowId);
      blurRecord.setFamily("fam0");
      blurRecord.addColumn("col0", "value-" + rowId);
      TableBlurRecord tableBlurRecord = new TableBlurRecord(new Text(tableName), blurRecord);
      assertEquals(tableBlurRecord, e.getValue());

      rowId++;
    }
    assertEquals(200, rowId);
  }

  public interface ResultReader {

    void read(Text rowId, TableBlurRecord tableBlurRecord);

  }

  private void walkOutput(Path output, Configuration conf, ResultReader resultReader) throws IOException {
    FileSystem fileSystem = output.getFileSystem(conf);
    FileStatus fileStatus = fileSystem.getFileStatus(output);
    if (fileStatus.isDir()) {
      FileStatus[] listStatus = fileSystem.listStatus(output, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return !path.getName().startsWith("_");
        }
      });
      for (FileStatus fs : listStatus) {
        walkOutput(fs.getPath(), conf, resultReader);
      }
    } else {
      Reader reader = new SequenceFile.Reader(fileSystem, output, conf);
      Text rowId = new Text();
      TableBlurRecord tableBlurRecord = new TableBlurRecord();
      while (reader.next(rowId, tableBlurRecord)) {
        resultReader.read(rowId, tableBlurRecord);
      }
      reader.close();
    }
  }

  private Iface getClient() {
    return BlurClient.getClientFromZooKeeperConnectionStr(miniCluster.getZkConnectionString());
  }

  private void loadTable(String tableName, int startId, int numb) throws BlurException, TException {
    Iface client = getClient();
    List<RowMutation> batch = new ArrayList<RowMutation>();
    for (int i = 0; i < numb; i++) {
      int id = startId + i;
      RowMutation rowMutation = new RowMutation();
      rowMutation.setTable(tableName);
      rowMutation.setRowId("row-" + Integer.toString(id));
      Record record = new Record();
      record.setFamily("fam0");
      record.setRecordId("record-" + id);
      record.addToColumns(new Column("col0", "value-" + id));
      rowMutation.addToRecordMutations(new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, record));
      batch.add(rowMutation);
    }
    client.mutateBatch(batch);
  }

  private void creatTable(String tableName, Path tables, boolean fastDisable) throws BlurException, TException {
    Path tablePath = new Path(tables, tableName);
    Iface client = getClient();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setTableUri(tablePath.toString());
    tableDescriptor.setName(tableName);
    tableDescriptor.setShardCount(2);
    tableDescriptor.putToTableProperties(BlurConstants.BLUR_TABLE_DISABLE_FAST_DIR, Boolean.toString(fastDisable));
    client.createTable(tableDescriptor);

    ColumnDefinition colDef = new ColumnDefinition();
    colDef.setFamily("fam0");
    colDef.setColumnName("col0");
    colDef.setFieldType("string");
    client.addColumnDefinition(tableName, colDef);
  }

  public static class TestMapper extends Mapper<Text, TableBlurRecord, Text, TableBlurRecord> {
    @Override
    protected void map(Text key, TableBlurRecord value, Context context) throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

}
