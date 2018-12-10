package org.apache.blur.spark;

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
import org.apache.spark.HashPartitioner;

public class BlurSparkPartitioner extends HashPartitioner {

  private static final long serialVersionUID = 9853263327838L;

  private final int _totalShard;

  public BlurSparkPartitioner(int partitions) {
    super(partitions);
    _totalShard = partitions;
  }

  @Override
  public int getPartition(Object key) {
    if (key instanceof Text) {
      return (key.hashCode() & Integer.MAX_VALUE) % _totalShard;
    } else {
      return super.getPartition(key);
    }
  }
}
