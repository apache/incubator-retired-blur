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

import org.apache.blur.mapreduce.lib.BlurMutate;
import org.apache.blur.mapreduce.lib.BlurMutate.MUTATE_TYPE;
import org.apache.blur.mapreduce.lib.update.IndexKey.TYPE;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.mapreduce.lib.GetCounter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class UpdateReducer extends Reducer<IndexKey, IndexValue, Text, BlurMutate> {

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
  protected void reduce(IndexKey key, Iterable<IndexValue> values, Context context) throws IOException,
      InterruptedException {
    if (key.getType() != TYPE.NEW_DATA_MARKER) {
      // There is no new data for this row, skip.
      return;
    } else {
      BlurRecord prevBlurRecord = null;
      String prevRecordId = null;
      for (IndexValue value : values) {
        BlurRecord br = value.getBlurRecord();
        if (br == null) {
          // Skip null records because there were likely many new data markers
          // for the row.
          continue;
        }

        // Safe Copy
        BlurRecord blurRecord = new BlurRecord(br);
        String recordId = blurRecord.getRecordId();
        if (prevRecordId == null || prevRecordId.equals(recordId)) {
          // reassign to new record.
          prevBlurRecord = blurRecord;
          prevRecordId = recordId;
        } else {
          // flush prev and assign
          context.write(new Text(blurRecord.getRowId()), toMutate(blurRecord));
        }
      }
      if (prevBlurRecord != null) {
        context.write(new Text(prevBlurRecord.getRowId()), toMutate(prevBlurRecord));
      }
    }
  }

  private BlurMutate toMutate(BlurRecord blurRecord) {
    return new BlurMutate(MUTATE_TYPE.REPLACE, blurRecord);
  }

}
