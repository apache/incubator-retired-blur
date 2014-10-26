/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.blur.hive;

import java.io.IOException;
import java.util.Properties;

import org.apache.blur.mapreduce.lib.BlurMutate;
import org.apache.blur.mapreduce.lib.BlurMutate.MUTATE_TYPE;
import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.mapreduce.lib.CheckOutputSpecs;
import org.apache.blur.mapreduce.lib.GenericBlurRecordWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;

public class BlurHiveOutputFormat implements HiveOutputFormat<Text, BlurRecord> {

  @Override
  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
    try {
      CheckOutputSpecs.checkOutputSpecs(jobConf, jobConf.getNumReduceTasks());
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public RecordWriter<Text, BlurRecord> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String name,
      Progressable progressable) throws IOException {
    throw new RuntimeException("Should never be called.");
  }

  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
      Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
      Progressable progress) throws IOException {
    TaskAttemptID taskAttemptID = TaskAttemptID.forName(jc.get("mapred.task.id"));
    int id = taskAttemptID.getTaskID().getId();
    final GenericBlurRecordWriter writer = new GenericBlurRecordWriter(jc, id, taskAttemptID.toString() + ".tmp");
    return new org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter() {

      @Override
      public void write(Writable w) throws IOException {
        BlurRecord blurRecord = (BlurRecord) w;
        BlurMutate blurMutate = new BlurMutate(MUTATE_TYPE.REPLACE, blurRecord);
        writer.write(new Text(blurRecord.getRowId()), blurMutate);
      }

      @Override
      public void close(boolean abort) throws IOException {
        writer.close();
      }
    };
  }

}
