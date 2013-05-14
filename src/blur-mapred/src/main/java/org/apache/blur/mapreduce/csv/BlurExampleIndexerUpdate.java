package org.apache.blur.mapreduce.csv;

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

import org.apache.blur.manager.clusterstatus.ZookeeperClusterStatus;
import org.apache.blur.mapreduce.BlurTask;
import org.apache.blur.mapreduce.BlurTask.INDEXING_TYPE;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class BlurExampleIndexerUpdate {

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration configuration = new Configuration();
    String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: blurindexer <in> <out>");
      System.exit(2);
    }

    ZookeeperClusterStatus status = new ZookeeperClusterStatus("localhost");
    TableDescriptor descriptor = status.getTableDescriptor(false, "default", "test-table");

    BlurTask blurTask = new BlurTask();
    blurTask.setTableDescriptor(descriptor);
    blurTask.setIndexingType(INDEXING_TYPE.UPDATE);
    Job job = blurTask.configureJob(configuration);
    job.setJarByClass(BlurExampleIndexerUpdate.class);
    job.setMapperClass(CsvBlurMapper.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1], "job-" + System.currentTimeMillis()));
    boolean waitForCompletion = job.waitForCompletion(true);
    System.exit(waitForCompletion ? 0 : 1);
  }
}
