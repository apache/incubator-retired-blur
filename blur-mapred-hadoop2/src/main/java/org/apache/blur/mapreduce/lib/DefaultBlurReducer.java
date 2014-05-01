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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This class is to be used in conjunction with {@link BlurOutputFormat}
 * .</br></br>
 * 
 * Here is a basic example of how to use both the {@link BlurOutputFormat} and
 * the {@link DefaultBlurReducer} together to build indexes.</br></br>
 * 
 * Once this job has successfully completed the indexes will be imported by the
 * running shard servers and be placed online. This is a polling mechicism in
 * the shard servers and by default they poll every 10 seconds.
 * 
 * 
 * </br></br>
 * 
 * Job job = new Job(conf, "blur index");</br>
 * job.setJarByClass(BlurOutputFormatTest.class);</br>
 * job.setMapperClass(CsvBlurMapper.class);</br>
 * job.setReducerClass(DefaultBlurReducer.class);</br>
 * job.setNumReduceTasks(1);</br>
 * job.setInputFormatClass(TrackingTextInputFormat.class);</br>
 * job.setOutputKeyClass(Text.class);
 * </br>job.setOutputValueClass(BlurMutate.class);</br>
 * job.setOutputFormatClass(BlurOutputFormat.class);</br> </br>
 * FileInputFormat.addInputPath(job, new Path(TEST_ROOT_DIR + "/in"));</br>
 * CsvBlurMapper.addColumns(job, "cf1", "col");</br> </br> TableDescriptor
 * tableDescriptor = new TableDescriptor();</br>
 * tableDescriptor.setShardCount(1)
 * ;</br>tableDescriptor.setAnalyzerDefinition(new
 * AnalyzerDefinition());</br>tableDescriptor.setTableUri(new Path(TEST_ROOT_DIR
 * + "/out").toString());</br>BlurOutputFormat.setTableDescriptor(job,
 * tableDescriptor);</br>
 * 
 * 
 */
public class DefaultBlurReducer extends Reducer<Writable, BlurMutate, Writable, BlurMutate> {

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    BlurOutputFormat.setProgressable(context);
    BlurOutputFormat.setGetCounter(new GetCounter() {
      @Override
      public Counter getCounter(Enum<?> counterName) {
        return context.getCounter(counterName);
      }
    });
  }

  @Override
  protected void reduce(Writable key, Iterable<BlurMutate> values, Context context) throws IOException,
      InterruptedException {
    Text textKey = getTextKey(key);
    for (BlurMutate value : values) {
      context.write(textKey, value);
    }
  }

  protected Text getTextKey(Writable key) {
    if (key instanceof Text) {
      return (Text) key;
    }
    throw new IllegalArgumentException("Key is not of type Text, you will need to "
        + "override DefaultBlurReducer and implement \"getTextKey\" method.");
  }
}
