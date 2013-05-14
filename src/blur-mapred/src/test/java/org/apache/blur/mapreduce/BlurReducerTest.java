package org.apache.blur.mapreduce;

import java.io.IOException;

import org.apache.blur.mapreduce.BlurMutate.MUTATE_TYPE;
import org.apache.blur.mapreduce.csv.CsvBlurMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class BlurReducerTest {

  MapDriver<LongWritable, Text, Text, BlurMutate> mapDriver;
  ReduceDriver<Text, BlurMutate, Text, BlurMutate> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, BlurMutate, Text, BlurMutate> mapReduceDriver;

  @Before
  public void setUp() throws IOException {
    CsvBlurMapper mapper = new CsvBlurMapper();

    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration configuration = mapDriver.getConfiguration();
    CsvBlurMapper.addColumns(configuration, "cf1", "col1", "col2");

    // Configuration configuration = new Configuration();
    // BlurTask blurTask = new BlurTask();
    // blurTask.configureJob(configuration);
    // mapDriver.setConfiguration(configuration);
    BlurReducer reducer = new BlurReducer();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);

    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

  }

  @Test
  public void testMapper() {
    mapDriver.withInput(new LongWritable(), new Text("rowid1,record1,cf1,value1,value2"));
    mapDriver.withOutput(new Text("rowid1"), new BlurMutate(MUTATE_TYPE.REPLACE, "rowid1", "record1", "cf1").addColumn("col1", "value1").addColumn("col2", "value2"));
    mapDriver.runTest();
  }

  @Test
  public void testReducer() {
    // List<IntWritable> values = new ArrayList<IntWritable>();
    // values.add(new IntWritable(1));
    // values.add(new IntWritable(1));
    // reduceDriver.withInput(new Text("6"), values);
    // reduceDriver.withOutput(new Text("6"), new IntWritable(2));
    // reduceDriver.runTest();
  }
}