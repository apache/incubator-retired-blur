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
package org.apache.blur.mapreduce.lib.v2;

import java.io.IOException;

import org.apache.blur.analysis.FieldManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DirectIndexingDriver implements Tool {

  public static class DirectIndexingMapper extends
      Mapper<IntWritable, DocumentWritable, LuceneKeyWritable, NullWritable> {

    private FieldManager _fieldManager;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(IntWritable key, DocumentWritable value, Context context) throws IOException,
        InterruptedException {
      int documentId = key.get();

    }

  }

  private Configuration _conf;

  @Override
  public Configuration getConf() {
    return _conf;
  }

  @Override
  public void setConf(Configuration conf) {
    _conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {

    String in = args[0];

    Job job = new Job(getConf(), "Lucene Direct Indexing");
    job.setJarByClass(DirectIndexingDriver.class);
    job.setMapperClass(DirectIndexingMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setOutputFormatClass(NullOutputFormat.class);
    job.setOutputKeyClass(LuceneKeyWritable.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(in));

    if (!job.waitForCompletion(true)) {
      return 1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DFSAdmin(), args));
  }

}
