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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.store.blockcache_v2.BaseCache.STORE;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleCacheValueBufferPoolTest {

  private static final int QUEUE_DEPTH = 100;

  private SimpleCacheValueBufferPool _pool;

  @Before
  public void setup() {
    _pool = new SimpleCacheValueBufferPool(STORE.ON_HEAP, QUEUE_DEPTH);
  }

  @After
  public void teardown() throws IOException {
    _pool.close();
  }

  @Test
  public void test1() {
    long count = _pool._created.count();
    CacheValue cacheValue1 = _pool.getCacheValue(1);
    assertEquals(count + 1L, _pool._created.count());
    _pool.returnToPool(cacheValue1);
    long count2 = _pool._reused.count();
    CacheValue cacheValue2 = _pool.getCacheValue(1);
    assertEquals(count2 + 1L, _pool._reused.count());
    _pool.returnToPool(cacheValue2);
  }

  @Test
  public void test2() {
    List<CacheValue> list = new ArrayList<CacheValue>();
    long count = _pool._created.count();
    int total = 10000;
    for (int i = 0; i < total; i++) {
      list.add(_pool.getCacheValue(10000));
    }
    assertEquals(count + total, _pool._created.count());
    long count2 = _pool._detroyed.count();
    for (CacheValue cacheValue : list) {
      _pool.returnToPool(cacheValue);
    }
    assertEquals(count2 + (total - QUEUE_DEPTH), _pool._detroyed.count());
  }
}
