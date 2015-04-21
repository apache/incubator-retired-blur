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
package org.apache.blur.mapreduce.lib.update;

import java.io.IOException;

import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.mapreduce.lib.TableBlurRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForExistingData extends Mapper<Text, TableBlurRecord, IndexKey, IndexValue> {
  
  

  private Counter _existingRecords;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    _existingRecords = context.getCounter("Blur", "Existing Records Read");
  }

  @Override
  protected void map(Text key, TableBlurRecord value, Context context) throws IOException, InterruptedException {
    BlurRecord blurRecord = value.getBlurRecord();
    IndexKey oldDataKey = IndexKey.oldData(blurRecord.getRowId(), blurRecord.getRecordId());
    context.write(oldDataKey, new IndexValue(blurRecord));
    _existingRecords.increment(1L);
  }

}
