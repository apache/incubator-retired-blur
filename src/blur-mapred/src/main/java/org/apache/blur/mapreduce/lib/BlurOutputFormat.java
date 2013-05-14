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
import java.io.IOException;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.mapreduce.BlurMutate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class BlurOutputFormat extends OutputFormat<Text, BlurMutate> {

  private static final String MAPRED_OUTPUT_COMMITTER_CLASS = "mapred.output.committer.class";
  public static final String BLUR_OUTPUT_PATH = "blur.output.path";

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
  }

  @Override
  public RecordWriter<Text, BlurMutate> getRecordWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {
    int id = context.getTaskAttemptID().getTaskID().getId();
    TaskAttemptID taskAttemptID = context.getTaskAttemptID();
    return new BlurRecordWriter(context.getConfiguration(), new BlurAnalyzer(), id, taskAttemptID.toString() + ".tmp");
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    int numReduceTasks = context.getNumReduceTasks();
    if (numReduceTasks != 0) {
      try {
        Class<? extends OutputFormat<?, ?>> outputFormatClass = context.getOutputFormatClass();
        if (outputFormatClass.equals(BlurOutputFormat.class)) {
          // Then only reducer needs committer.
          if (context.getTaskAttemptID().isMap()) {
            return getDoNothing();
          }
        }
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
    return new BlurOutputCommitter();
  }

  private OutputCommitter getDoNothing() {
    return new OutputCommitter() {
      
      @Override
      public void commitJob(JobContext jobContext) throws IOException {
      }

      @Override
      public void cleanupJob(JobContext context) throws IOException {
      }

      @Override
      public void abortJob(JobContext jobContext, State state) throws IOException {
      }

      @Override
      public void setupTask(TaskAttemptContext taskContext) throws IOException {
        
      }
      
      @Override
      public void setupJob(JobContext jobContext) throws IOException {
        
      }
      
      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return false;
      }
      
      @Override
      public void commitTask(TaskAttemptContext taskContext) throws IOException {
        
      }
      
      @Override
      public void abortTask(TaskAttemptContext taskContext) throws IOException {
        
      }
    };
  }

  public static void setOutputPath(Job job, Path path) {
    Configuration configuration = job.getConfiguration();
    configuration.set(BLUR_OUTPUT_PATH, path.toString());
    configuration.set(MAPRED_OUTPUT_COMMITTER_CLASS, BlurOutputCommitter.class.getName());
  }

  public static Path getOutputPath(Configuration configuration) {
    return new Path(configuration.get(BLUR_OUTPUT_PATH));
  }

}
