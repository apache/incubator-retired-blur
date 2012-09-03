package org.apache.blur.mapred;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.mapreduce.BlurRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class BlurInputFormat implements InputFormat<Text, BlurRecord> {

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<?> splits = new ArrayList<Object>();
    Path[] paths = FileInputFormat.getInputPaths(job);
    for (Path path : paths) {
      org.apache.blur.mapreduce.lib.BlurInputFormat.findAllSegments((Configuration) job, path, splits);
    }
    return splits.toArray(new InputSplit[] {});
  }

  @Override
  public RecordReader<Text, BlurRecord> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    return new BlurRecordReader(split, job);
  }

}
