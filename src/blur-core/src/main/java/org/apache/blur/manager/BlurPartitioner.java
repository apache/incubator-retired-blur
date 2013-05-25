package org.apache.blur.manager;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Simple hashing class used to guide the rows to the correct shards during
 * MapReduce jobs as well as during normal runtime operations.
 */
public class BlurPartitioner extends Partitioner<Text, Writable> {

  /**
   * Gets the shard from the the rowId, based on the number of shards in the
   * table.
   * 
   * @param rowId
   *          the rowId
   * @param numberOfShardsInTable
   *          the number of shards in the table.
   * @return the shard where this rowId should be stored.
   */
  public int getShard(String rowId, int numberOfShardsInTable) {
    return getShard(new Text(rowId), numberOfShardsInTable);
  }

  /**
   * Gets the shard from the the rowId, based on the number of shards in the
   * table.
   * 
   * @param rowId
   *          the rowId
   * @param numberOfShardsInTable
   *          the number of shards in the table.
   * @return the shard where this rowId should be stored.
   */
  public int getShard(Text rowId, int numberOfShardsInTable) {
    return getPartition(rowId, null, numberOfShardsInTable);
  }

  /**
   * Gets the partition or reducer from the the rowId, based on the number of
   * shards in the table.
   * 
   * @param rowId
   *          the rowId
   * @param numberOfShardsInTable
   *          the number of shards in the table.
   * @return the partition where this rowId should be processed.
   */
  @Override
  public int getPartition(Text key, Writable value, int numReduceTasks) {
    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }

}
