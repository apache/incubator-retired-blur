package org.apache.blur.hive;

import java.io.IOException;
import java.util.Properties;

import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

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

public class BlurHiveOutputFormat implements HiveOutputFormat<Text, BlurRecord> {

  @Override
  public void checkOutputSpecs(FileSystem arg0, JobConf arg1) throws IOException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public RecordWriter<Text, BlurRecord> getRecordWriter(FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
      throws IOException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
      Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
      Progressable progress) throws IOException {
    throw new RuntimeException("Not Implemented");
  }

}
