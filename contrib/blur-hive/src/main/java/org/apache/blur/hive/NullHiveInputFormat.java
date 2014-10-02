package org.apache.blur.hive;

import java.io.IOException;

import groovy.lang.Writable;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

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

public class NullHiveInputFormat implements InputFormat<Writable, Writable> {

  @Override
  public RecordReader<Writable, Writable> getRecordReader(InputSplit arg0, JobConf arg1, Reporter arg2)
      throws IOException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
    throw new RuntimeException("Not Implemented");
  }

}
