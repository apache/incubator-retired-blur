package com.nearinfinity.blur.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.nearinfinity.blur.mapreduce.BlurRecord;

public class BlurInputFormat implements InputFormat<Text, BlurRecord> {

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<?> splits = new ArrayList<Object>();
    Path[] paths = FileInputFormat.getInputPaths(job);
    for (Path path : paths) {
      com.nearinfinity.blur.mapreduce.lib.BlurInputFormat.findAllSegments((Configuration)job,path,splits);
    }
    return splits.toArray(new InputSplit[]{});
  }

  @Override
  public RecordReader<Text, BlurRecord> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    return new BlurRecordReader(split,job);
  }

}
