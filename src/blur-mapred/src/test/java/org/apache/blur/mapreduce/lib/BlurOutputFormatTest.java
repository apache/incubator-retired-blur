package org.apache.blur.mapreduce.lib;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.blur.mapreduce.BlurMutate;
import org.apache.blur.mapreduce.csv.CsvBlurMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TestMapReduceLocal.TrackingTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
    writeFile("in/part1", "1,1,cf1,val1");
    writeFile("in/part2", "1,2,cf1,val2");
    Job job = new Job(conf, "blur index");
    job.setJarByClass(BlurOutputFormatTest.class);
    job.setMapperClass(CsvBlurMapper.class);
    job.setReducerClass(DefaultBlurReducer.class);
    job.setNumReduceTasks(4);
    job.setInputFormatClass(TrackingTextInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BlurMutate.class);
    job.setOutputFormatClass(BlurOutputFormat.class);

    CsvBlurMapper.addColumns(job, "cf1", "col");

    FileInputFormat.addInputPath(job, new Path(TEST_ROOT_DIR + "/in"));
    BlurOutputFormat.setOutputPath(job, new Path(TEST_ROOT_DIR + "/out"));

    assertTrue(job.waitForCompletion(true));
    Counters ctrs = job.getCounters();
    System.out.println("Counters: " + ctrs);
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

  public static Path writeFile(String name, String data) throws IOException {
    Path file = new Path(TEST_ROOT_DIR + "/" + name);
    localFs.delete(file, false);
    DataOutputStream f = localFs.create(file);
    f.write(data.getBytes());
    f.close();
    return file;
  }
}
