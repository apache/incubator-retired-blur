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

import java.io.IOException;
import java.util.List;

import org.apache.blur.store.blockcache.BlockLocks;
import org.apache.blur.store.blockcache_v2.BaseCache.STORE;

public class SlabAllocationCacheValueBufferPool extends BaseCacheValueBufferPool {

  public SlabAllocationCacheValueBufferPool(STORE store) {
    super(store);
  }

  static class Slab {
    final long _address;
    final BlockLocks _locks;

    Slab(long address, int numberOfBlocks) {
      _address = address;
      _locks = new BlockLocks(numberOfBlocks);
    }

    int findChunk() {
      throw new RuntimeException("Not implemented.");
    }
  }

  @Override
  public CacheValue getCacheValue(int cacheBlockSize) {
    
    List<Slab> slabs = getSlabs();
    for (Slab slab : slabs) {
      int chunkId = slab.findChunk();
    }
    throw new RuntimeException("Not implemented.");
  }

  private List<Slab> getSlabs() {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void returnToPool(CacheValue cacheValue) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void close() throws IOException {
    throw new RuntimeException("Not implemented.");
  }

}
