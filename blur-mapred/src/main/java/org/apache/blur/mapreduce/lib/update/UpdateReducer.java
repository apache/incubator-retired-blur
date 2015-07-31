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

  private static final String IGNORED_EXISTING_ROWS = "Ignored Existing Rows";
  private static final String MULTIPLE_RECORD_W_SAME_RECORD_ID = "Multiple Record w/ Same Record Id";
  private static final String INDEX_VALUES = "IndexValues";
  private static final String NULL_BLUR_RECORDS = "NULL Blur Records";
  private static final String MARKER_RECORDS = "Marker Records";
  private static final String SEP = " - ";
  private static final String BLUR_UPDATE = "Blur Update";
  private static final String EXISTING_RCORDS = "Existing Rcords";
  private static final String NEW_RCORDS = "New Rcords";
  private static final String NO_UPDATE = "NoUpdate";
  private static final String UPDATE = "Update";
  private static final String BLUR_UPDATE_DEBUG = BLUR_UPDATE + SEP + "DEBUG";

  private Counter _newRecordsUpdate;
  private Counter _newRecordsNoUpdate;
  private Counter _existingRecordsUpdate;
  private Counter _existingRecordsNoUpdate;
  private Counter _ignoredExistingRows;
  private Counter _debugRecordsWithSameRecordId;
  private Counter _debugMarkerRecordsNoUpdate;
  private Counter _debugMarkerRecordsUpdate;
  private Counter _debugIndexValues;
  private Counter _debugNullBlurRecords;

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    BlurOutputFormat.setProgressable(context);
    BlurOutputFormat.setGetCounter(new GetCounter() {
      @Override
      public Counter getCounter(Enum<?> counterName) {
        return context.getCounter(counterName);
      }
    });

    _newRecordsUpdate = context.getCounter(BLUR_UPDATE, NEW_RCORDS + SEP + UPDATE);
    _newRecordsNoUpdate = context.getCounter(BLUR_UPDATE, NEW_RCORDS + SEP + NO_UPDATE);
    _existingRecordsUpdate = context.getCounter(BLUR_UPDATE, EXISTING_RCORDS + SEP + UPDATE);
    _existingRecordsNoUpdate = context.getCounter(BLUR_UPDATE, EXISTING_RCORDS + SEP + NO_UPDATE);
    _ignoredExistingRows = context.getCounter(BLUR_UPDATE, IGNORED_EXISTING_ROWS);

    _debugRecordsWithSameRecordId = context.getCounter(BLUR_UPDATE_DEBUG, MULTIPLE_RECORD_W_SAME_RECORD_ID);

    _debugMarkerRecordsNoUpdate = context.getCounter(BLUR_UPDATE_DEBUG, MARKER_RECORDS + SEP + NO_UPDATE);
    _debugMarkerRecordsUpdate = context.getCounter(BLUR_UPDATE_DEBUG, MARKER_RECORDS + SEP + UPDATE);
    _debugIndexValues = context.getCounter(BLUR_UPDATE_DEBUG, INDEX_VALUES);
    _debugNullBlurRecords = context.getCounter(BLUR_UPDATE_DEBUG, NULL_BLUR_RECORDS);

  }

  @Override
  protected void reduce(IndexKey key, Iterable<IndexValue> values, Context context) throws IOException,
      InterruptedException {
    if (key.getType() != TYPE.NEW_DATA_MARKER) {
      handleNoNewData(key, values);
    } else {
      handleNewData(key, values, context);
    }
  }

  private void handleNewData(IndexKey key, Iterable<IndexValue> values, Context context) throws IOException,
      InterruptedException {
    BlurRecord prevBlurRecord = null;
    String prevRecordId = null;
    for (IndexValue value : values) {
      updateCounters(true, key);
      BlurRecord br = value.getBlurRecord();
      if (br == null) {
        // Skip null records because there were likely many new data markers
        // for the row.
        _debugNullBlurRecords.increment(1);
      } else {
        // Safe Copy
        BlurRecord currentBlurRecord = new BlurRecord(br);
        String currentRecordId = currentBlurRecord.getRecordId();
        if (prevRecordId != null) {
          if (prevRecordId.equals(currentRecordId)) {
            _debugRecordsWithSameRecordId.increment(1);
          } else {
            // flush prev
            context.write(new Text(prevBlurRecord.getRowId()), toMutate(prevBlurRecord));
          }
        }
        // assign
        prevBlurRecord = currentBlurRecord;
        prevRecordId = currentRecordId;
      }
    }
    if (prevBlurRecord != null) {
      context.write(new Text(prevBlurRecord.getRowId()), toMutate(prevBlurRecord));
    }
  }

  private void updateCounters(boolean update, IndexKey key) {
    _debugIndexValues.increment(1);
    if (update) {
      switch (key.getType()) {
      case NEW_DATA:
        _newRecordsUpdate.increment(1);
        break;
      case OLD_DATA:
        _existingRecordsUpdate.increment(1);
        break;
      case NEW_DATA_MARKER:
        _debugMarkerRecordsUpdate.increment(1);
      default:
        break;
      }
    } else {
      switch (key.getType()) {
      case NEW_DATA:
        _newRecordsNoUpdate.increment(1);
        break;
      case OLD_DATA:
        _existingRecordsNoUpdate.increment(1);
        break;
      case NEW_DATA_MARKER:
        _debugMarkerRecordsNoUpdate.increment(1);
      default:
        break;
      }
    }
  }

  private void handleNoNewData(IndexKey key, Iterable<IndexValue> values) {
    _ignoredExistingRows.increment(1);
    for (@SuppressWarnings("unused")
    IndexValue indexValue : values) {
      updateCounters(false, key);
    }
  }

  private BlurMutate toMutate(BlurRecord blurRecord) {
    return new BlurMutate(MUTATE_TYPE.REPLACE, blurRecord);
  }

}
