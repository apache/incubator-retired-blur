package com.nearinfinity.blur.mapreduce.lib;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.nearinfinity.blur.mapreduce.BlurRecord;

public class BlurOutputFormat extends OutputFormat<Text,BlurRecord> {
  
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    
  }

  @Override
  public RecordWriter<Text, BlurRecord> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new BlurRecordWriter(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new BlurOutputCommitter(context);
  }

}
