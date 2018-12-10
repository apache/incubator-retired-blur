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
package org.apache.blur.server.cache;

import java.util.Arrays;

public class ShardsKey {

  private final int[] _shards;

  public ShardsKey(int[] shards) {
    Arrays.sort(shards);
    _shards = shards;
  }

  public int[] getShards() {
    return _shards;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(_shards);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ShardsKey other = (ShardsKey) obj;
    if (!Arrays.equals(_shards, other._shards))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "ShardKey [_shards=" + Arrays.toString(_shards) + "]";
  }

}
