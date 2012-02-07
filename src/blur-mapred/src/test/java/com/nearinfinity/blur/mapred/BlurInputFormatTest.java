package com.nearinfinity.blur.mapred;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.mapreduce.BlurRecord;
import com.nearinfinity.blur.mapreduce.lib.BlurInputSplit;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurUtil;

public class BlurInputFormatTest {
  
  private Path indexPath = new Path("./tmp/test-indexes/oldapi");
  private int numberOfShards = 13;
  private int rowsPerIndex = 10;
  
  @Before
  public void setup() throws IOException {
    com.nearinfinity.blur.mapreduce.lib.BlurInputFormatTest.buildTestIndexes(indexPath,numberOfShards,rowsPerIndex);
  }
  
  @Test
  public void testGetSplits() throws IOException {
    BlurInputFormat format = new BlurInputFormat();
    JobConf job = new JobConf(new Configuration());
    FileInputFormat.addInputPath(job, indexPath);
    InputSplit[] splits = format.getSplits(job, -1);
    for (int i = 0; i < splits.length; i++) {
      BlurInputSplit split = (BlurInputSplit) splits[i];
      Path path = new Path(indexPath,BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, i));
      FileSystem fileSystem = path.getFileSystem(job);
      assertEquals(new BlurInputSplit(fileSystem.makeQualified(path), "_0", 0, Integer.MAX_VALUE), split);
    }
  }
  
  @Test
  public void testGetRecordReader() throws IOException {
    BlurInputFormat format = new BlurInputFormat();
    JobConf job = new JobConf(new Configuration());
    FileInputFormat.addInputPath(job, indexPath);
    InputSplit[] splits = format.getSplits(job, -1);
    for (int i = 0; i < splits.length; i++) {
      RecordReader<Text, BlurRecord> reader = format.getRecordReader(splits[i], job, Reporter.NULL);
      Text key = reader.createKey();
      BlurRecord value = reader.createValue();
      while (reader.next(key, value)) {
        System.out.println(reader.getProgress() + " " + key + " " + value);
      }
    }
  }

}
