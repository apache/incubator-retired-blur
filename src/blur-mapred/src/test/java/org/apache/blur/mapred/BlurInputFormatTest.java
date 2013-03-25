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
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.blur.mapred.BlurInputFormat;
import org.apache.blur.mapreduce.BlurRecord;
import org.apache.blur.mapreduce.lib.BlurInputSplit;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
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


public abstract class BlurInputFormatTest {

//  private Path indexPath = new Path(TMPDIR, "./tmp/test-indexes/oldapi");
//  private int numberOfShards = 13;
//  private int rowsPerIndex = 10;
//
//  @Before
//  public void setup() throws IOException {
//    org.apache.blur.mapreduce.lib.BlurInputFormatTest.buildTestIndexes(indexPath, numberOfShards, rowsPerIndex);
//  }
//
//  @Test
//  public void testGetSplits() throws IOException {
//    BlurInputFormat format = new BlurInputFormat();
//    JobConf job = new JobConf(new Configuration());
//    FileInputFormat.addInputPath(job, indexPath);
//    InputSplit[] splits = format.getSplits(job, -1);
//    for (int i = 0; i < splits.length; i++) {
//      BlurInputSplit split = (BlurInputSplit) splits[i];
//      Path path = new Path(indexPath, BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, i));
//      FileSystem fileSystem = path.getFileSystem(job);
//      assertEquals(new BlurInputSplit(fileSystem.makeQualified(path), "_0", 0, Integer.MAX_VALUE), split);
//    }
//  }
//
//  @Test
//  public void testGetRecordReader() throws IOException {
//    BlurInputFormat format = new BlurInputFormat();
//    JobConf job = new JobConf(new Configuration());
//    FileInputFormat.addInputPath(job, indexPath);
//    InputSplit[] splits = format.getSplits(job, -1);
//    for (int i = 0; i < splits.length; i++) {
//      RecordReader<Text, BlurRecord> reader = format.getRecordReader(splits[i], job, Reporter.NULL);
//      Text key = reader.createKey();
//      BlurRecord value = reader.createValue();
//      while (reader.next(key, value)) {
//        System.out.println(reader.getProgress() + " " + key + " " + value);
//      }
//    }
//  }

}
