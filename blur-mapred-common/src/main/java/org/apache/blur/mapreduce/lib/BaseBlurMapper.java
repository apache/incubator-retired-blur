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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Base mapper class for Blur map reduce classes.
 * 
 * @param <KEY>
 * @param <VALUE>
 */
public abstract class BaseBlurMapper<KEY, VALUE> extends Mapper<KEY, VALUE, Text, BlurMutate> {
  protected BlurMutate _mutate;
  protected Text _key;
  protected Counter _recordCounter;
  protected Counter _columnCounter;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    _mutate = new BlurMutate();
    _mutate.setRecord(new BlurRecord());
    _key = new Text();
    _recordCounter = context.getCounter(BlurCounters.RECORD_COUNT);
    _columnCounter = context.getCounter(BlurCounters.COLUMN_COUNT);
  }

  @Override
  protected abstract void map(KEY key, VALUE value, Context context) throws IOException, InterruptedException;

}
