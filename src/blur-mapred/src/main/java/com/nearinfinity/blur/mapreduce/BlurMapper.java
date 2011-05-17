/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class BlurMapper<KEY,VALUE> extends Mapper<KEY, VALUE, BytesWritable, BlurRecord> {

    protected BlurRecord record;
    protected BytesWritable key;
    protected BlurTask blurTask;
    protected Counter recordCounter;
    protected Counter fieldCounter;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        long maxRecordCount = blurTask.getMaxRecordCount();
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
        blurTask = new BlurTask(context);
        record = new BlurRecord();
        key = new BytesWritable();
        recordCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getRecordCounterName());
        fieldCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getFieldCounterName());
    }

    @Override
    protected abstract void map(KEY key, VALUE value, Context context) throws IOException, InterruptedException;

}
