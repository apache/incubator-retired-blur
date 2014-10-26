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
package org.apache.blur.hive;

import org.apache.hadoop.hive.ql.io.HivePartitioner;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class BlurHiveParitioner implements Partitioner<Writable, Writable>, HivePartitioner<Writable, Writable> {

  @Override
  public void configure(JobConf job) {
  }

  @Override
  public int getPartition(Writable key, Writable value, int numPartitions) {
    if (value instanceof BytesWritable) {
      Text rowId = getRowId((BytesWritable) value);
      return (rowId.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
    throw new RuntimeException("Value of [" + value + "] is not supported.");
  }

  private Text getRowId(BytesWritable value) {
    byte[] bs = value.getBytes();
    int starting = find(bs, (byte) 1, 0);
    int ending = find(bs, (byte) 1, starting + 1);
    Text text = new Text();
    // 00 01 30 31 01
    // 0 1 2 3 4
    // starting = 1
    // ending = 4
    starting++;
    text.set(bs, starting, ending - starting);
    return text;
  }

  private int find(byte[] bs, byte b, int pos) {
    for (int i = pos; i < bs.length; i++) {
      if (bs[i] == b) {
        return i;
      }
    }
    throw new RuntimeException("Seperator [" + b + "] not found.");
  }

  @Override
  public int getBucket(Writable key, Writable value, int numBuckets) {
    return getPartition(key, value, numBuckets);
  }

}
