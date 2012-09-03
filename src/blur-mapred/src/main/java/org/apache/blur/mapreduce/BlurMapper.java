package org.apache.blur.mapreduce;

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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class BlurMapper<KEY, VALUE> extends Mapper<KEY, VALUE, BytesWritable, BlurMutate> {

  protected BlurMutate _mutate;
  protected BytesWritable _key;
  protected BlurTask _blurTask;
  protected Counter _recordCounter;
  protected Counter _fieldCounter;

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    long maxRecordCount = _blurTask.getMaxRecordCount();
    if (maxRecordCount == -1) {
      maxRecordCount = Long.MAX_VALUE;
    }
    for (long l = 0; l < maxRecordCount && context.nextKeyValue(); l++) {
      map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    _blurTask = BlurTask.read(context.getConfiguration());
    _mutate = new BlurMutate();
    _key = new BytesWritable();
    _recordCounter = context.getCounter(BlurTask.getCounterGroupName(), BlurTask.getRecordCounterName());
    _fieldCounter = context.getCounter(BlurTask.getCounterGroupName(), BlurTask.getFieldCounterName());
  }

  @Override
  protected abstract void map(KEY key, VALUE value, Context context) throws IOException, InterruptedException;

}
