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

package com.nearinfinity.blur.mapreduce.example;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.nearinfinity.blur.mapreduce.BlurMapper;

public class BlurExampleMapper extends BlurMapper {

    @Override
    protected void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException {
        record.clearColumns();
        String str = value.toString();
        String[] split = str.split("\\t");
        record.setRowId(UUID.randomUUID().toString());
        record.setRecordId(UUID.randomUUID().toString());
        record.setColumnFamily("cf1");
        for (int i = 0; i < split.length; i++) {
            record.addColumn("c"+i,split[i]);
            fieldCounter.increment(1);
        }
        byte[] bs = record.getRowId().getBytes();
        key.set(bs, 0, bs.length);
        context.write(key, record);
        recordCounter.increment(1);
        context.progress();
    }
}
