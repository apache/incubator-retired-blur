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
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TestMapReduceLocal.TrackingTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.lucene.index.DirectoryReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlurOutputFormatTest {

  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  private static MiniMRCluster mr;
  private static Path TEST_ROOT_DIR;

  @BeforeClass
  public static void setup() throws Exception {
    System.setProperty("test.build.data", "./target/BlurOutputFormatTest/data");
    TEST_ROOT_DIR = new Path(System.getProperty("test.build.data", "/tmp"));
    System.setProperty("hadoop.log.dir", "./target/BlurOutputFormatTest/hadoop_log");
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
    mr = new MiniMRCluster(2, "file:///", 3);
    BufferStore.init(128, 128);
  }

  @AfterClass
  public static void teardown() {
    if (mr != null) {
      mr.shutdown();
    }
  }

  @Test
  public void testBlurOutputFormat() throws IOException, InterruptedException, ClassNotFoundException {
    localFs.delete(new Path(TEST_ROOT_DIR + "/in"), true);
    localFs.delete(new Path(TEST_ROOT_DIR + "/out"), true);
    writeRecordsFile("in/part1", 1, 1, 1, 1, "cf1");
    writeRecordsFile("in/part2", 1, 1, 2, 1, "cf1");

    Job job = new Job(conf, "blur index");
    job.setJarByClass(BlurOutputFormatTest.class);
    job.setMapperClass(CsvBlurMapper.class);
    job.setReducerClass(DefaultBlurReducer.class);
    job.setNumReduceTasks(1);
    job.setInputFormatClass(TrackingTextInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BlurMutate.class);
    job.setOutputFormatClass(BlurOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(TEST_ROOT_DIR + "/in"));
    String tableUri = new Path(TEST_ROOT_DIR + "/out").toString();
    CsvBlurMapper.addColumns(job, "cf1", "col");

    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setShardCount(1);
    tableDescriptor.setAnalyzerDefinition(new AnalyzerDefinition());
    tableDescriptor.setTableUri(tableUri);
    BlurOutputFormat.setTableDescriptor(job, tableDescriptor);

    assertTrue(job.waitForCompletion(true));
    Counters ctrs = job.getCounters();
    System.out.println("Counters: " + ctrs);

    DirectoryReader reader = DirectoryReader.open(new HdfsDirectory(conf, new Path(new Path(tableUri, BlurUtil
        .getShardName(0)), "attempt_local_0001_r_000000_0.commit")));
    assertEquals(2, reader.numDocs());
    reader.close();
  }

  @Test
  public void testBlurOutputFormatOverFlowTest() throws IOException, InterruptedException, ClassNotFoundException {
    localFs.delete(new Path(TEST_ROOT_DIR + "/in"), true);
    localFs.delete(new Path(TEST_ROOT_DIR + "/out"), true);

    writeRecordsFile("in/part1", 1, 50, 1, 1500, "cf1"); // 1500 * 50 = 75,000
    writeRecordsFile("in/part2", 1, 50, 2000, 100, "cf1"); // 100 * 50 = 5,000

    Job job = new Job(conf, "blur index");
    job.setJarByClass(BlurOutputFormatTest.class);
    job.setMapperClass(CsvBlurMapper.class);
    job.setReducerClass(DefaultBlurReducer.class);
    job.setNumReduceTasks(1);
    job.setInputFormatClass(TrackingTextInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BlurMutate.class);
    job.setOutputFormatClass(BlurOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(TEST_ROOT_DIR + "/in"));
    String tableUri = new Path(TEST_ROOT_DIR + "/out").toString();
    CsvBlurMapper.addColumns(job, "cf1", "col");

    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setShardCount(1);
    tableDescriptor.setAnalyzerDefinition(new AnalyzerDefinition());
    tableDescriptor.setTableUri(tableUri);
    BlurOutputFormat.setTableDescriptor(job, tableDescriptor);

    assertTrue(job.waitForCompletion(true));
    Counters ctrs = job.getCounters();
    System.out.println("Counters: " + ctrs);

    DirectoryReader reader = DirectoryReader.open(new HdfsDirectory(conf, new Path(new Path(tableUri, BlurUtil
        .getShardName(0)), "attempt_local_0002_r_000000_0.commit")));
    assertEquals(80000, reader.numDocs());
    reader.close();
  }

  public static String readFile(String name) throws IOException {
    DataInputStream f = localFs.open(new Path(TEST_ROOT_DIR + "/" + name));
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
    localFs.delete(file, false);
    DataOutputStream f = localFs.create(file);
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
