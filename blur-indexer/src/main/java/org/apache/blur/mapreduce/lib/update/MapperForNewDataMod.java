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
import java.lang.reflect.Field;

import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapperForNewDataMod extends Mapper<Text, BlurRecord, IndexKey, IndexValue> {

  private static final IndexValue EMPTY_RECORD = new IndexValue();
  private long _timestamp;
  private Counter _newRecords;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    InputSplit inputSplit = context.getInputSplit();
    FileSplit fileSplit = getFileSplit(inputSplit);
    Path path = fileSplit.getPath();
    FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
    FileStatus fileStatus = fileSystem.getFileStatus(path);
    _timestamp = fileStatus.getModificationTime();
    _newRecords = context.getCounter(BlurIndexCounter.NEW_RECORDS);
  }

  private FileSplit getFileSplit(InputSplit inputSplit) throws IOException {
    if (inputSplit instanceof FileSplit) {
      return (FileSplit) inputSplit;
    }
    if (inputSplit.getClass().getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
      try {
        Field declaredField = inputSplit.getClass().getDeclaredField("inputSplit");
        declaredField.setAccessible(true);
        return getFileSplit((InputSplit) declaredField.get(inputSplit));
      } catch (NoSuchFieldException e) {
        throw new IOException(e);
      } catch (SecurityException e) {
        throw new IOException(e);
      } catch (IllegalArgumentException e) {
        throw new IOException(e);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      }
    } else {
      throw new IOException("Unknown input split type [" + inputSplit + "] [" + inputSplit.getClass() + "]");
    }
  }

  @Override
  protected void map(Text key, BlurRecord blurRecord, Context context) throws IOException, InterruptedException {
    IndexKey newDataKey = IndexKey.newData(blurRecord.getRowId(), blurRecord.getRecordId(), _timestamp);
    context.write(newDataKey, new IndexValue(blurRecord));
    _newRecords.increment(1L);

    IndexKey newDataMarker = IndexKey.newDataMarker(blurRecord.getRowId());
    context.write(newDataMarker, EMPTY_RECORD);
  }

}
