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
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SlabAllocationCacheValueBufferPoolTest {

  private SlabAllocationCacheValueBufferPool _pool;

  @Before
  public void setup() {
    int chunkSize = 1000;
    int slabSize = chunkSize * 10000;
    _pool = new SlabAllocationCacheValueBufferPool(chunkSize, slabSize);
  }

  @After
  public void teardown() throws IOException {
    _pool.close();
  }

  @Test
  public void test1() {
    CacheValue cacheValue1 = _pool.getCacheValue(1);
    _pool.returnToPool(cacheValue1);
    CacheValue cacheValue2 = _pool.getCacheValue(1);
    _pool.returnToPool(cacheValue2);
  }

  @Test
  public void test2() {
    List<CacheValue> list = new ArrayList<CacheValue>();
    int total = 10000;
    for (int i = 0; i < total; i++) {
      list.add(_pool.getCacheValue(10000));
    }
    for (CacheValue cacheValue : list) {
      _pool.returnToPool(cacheValue);
    }
  }
}
