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
package org.apache.blur.store.blockcache_v2;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

public class LRUIndexInputCache extends IndexInputCache {

  private final ConcurrentLinkedHashMap<Long, CacheValue> _cacheMap;

  public LRUIndexInputCache(long fileLength, int cacheBlockSize, int entries, MeterWrapper hits) {
    super(fileLength, cacheBlockSize, hits);
    _cacheMap = new ConcurrentLinkedHashMap.Builder<Long, CacheValue>().maximumWeightedCapacity(entries).build();
  }

  @Override
  public void put(long blockId, CacheValue cacheValue) {
    _cacheMap.put(blockId, cacheValue);
  }

  @Override
  public CacheValue get(long blockId) {
    CacheValue cacheValue = _cacheMap.get(blockId);
    if (cacheValue == null) {
      return null;
    } else if (cacheValue.isEvicted()) {
      _cacheMap.remove(blockId, cacheValue);
      return null;
    }
    hit();
    return cacheValue;
  }

}
