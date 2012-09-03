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
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.blur.mapreduce.BlurRecord;
import org.apache.blur.mapreduce.lib.BlurRecordWriter;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.lucene.index.IndexReader;
import org.junit.Test;


public class BlurRecordWriterTest {

  @Test
  public void testBlurRecordWriter() throws IOException, InterruptedException {
    JobID jobId = new JobID();
    TaskID tId = new TaskID(jobId, false, 13);
    TaskAttemptID taskId = new TaskAttemptID(tId, 0);
    Configuration conf = new Configuration();
    String pathStr = "./tmp/output-record-writer-test-newapi";
    rm(new File(pathStr));
    conf.set("mapred.output.dir", pathStr);
    TaskAttemptContext context = new TaskAttemptContext(conf, taskId);
    BlurRecordWriter writer = new BlurRecordWriter(context);

    Text key = new Text();
    BlurRecord value = new BlurRecord();

    for (int i = 0; i < 10; i++) {
      String rowId = UUID.randomUUID().toString();
      key.set(rowId);
      value.setFamily("cf");
      value.setRowId(rowId);
      value.setRecordId(UUID.randomUUID().toString());
      value.addColumn("name", "value");
      writer.write(key, value);
    }

    writer.close(context);

    // assert index exists and has document

    HdfsDirectory dir = new HdfsDirectory(new Path(pathStr, BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, 13)));
    assertTrue(IndexReader.indexExists(dir));
    IndexReader reader = IndexReader.open(dir);
    assertEquals(10, reader.numDocs());
  }

  private void rm(File file) {
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }

}
