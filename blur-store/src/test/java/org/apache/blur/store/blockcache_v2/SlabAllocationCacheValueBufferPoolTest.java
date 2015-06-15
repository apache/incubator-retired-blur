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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.blur.store.blockcache_v2.cachevalue.ByteArrayCacheValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SlabAllocationCacheValueBufferPoolTest {

  private SlabAllocationCacheValueBufferPool _pool;
  private int _chunkSize;
  private int _slabSize;

  @Before
  public void setup() {
    _chunkSize = 1000;
    _slabSize = _chunkSize * 10000;
    _pool = new SlabAllocationCacheValueBufferPool(_chunkSize, _slabSize);
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
    assertEquals(_slabSize, _pool.getCurrentSize());
  }

  @Test
  public void test2() {
    List<CacheValue> list = new ArrayList<CacheValue>();
    int total = 10000;
    int cacheBlockSize = 10000;
    for (int i = 0; i < total; i++) {
      list.add(_pool.getCacheValue(cacheBlockSize));
    }
    assertEquals((long) total * (long) cacheBlockSize, _pool.getCurrentSize());
    // _pool.dumpSlabPopulation();
    list.add(_pool.getCacheValue(cacheBlockSize));
    assertEquals(((long) total * (long) cacheBlockSize) + _slabSize, _pool.getCurrentSize());
    for (CacheValue cacheValue : list) {
      _pool.returnToPool(cacheValue);
    }
  }

  @Test
  public void test3() throws EvictionException {
    Random random = new Random(1);
    List<CacheValue> list = new ArrayList<CacheValue>();
    List<CacheValue> copy = new ArrayList<CacheValue>();
    int total = 10000;
    int cacheBlockSize = 10000;
    for (int i = 0; i < total; i++) {
      CacheValue cacheValue = _pool.getCacheValue(cacheBlockSize);
      ByteArrayCacheValue copyCv = new ByteArrayCacheValue(cacheBlockSize);
      populate(random, cacheValue, copyCv);
      list.add(cacheValue);
      copy.add(copyCv);
    }
    assertEquals((long) total * (long) cacheBlockSize, _pool.getCurrentSize());
    for (int i = 0; i < list.size(); i++) {
      CacheValue cv1 = list.get(i);
      CacheValue cv2 = copy.get(i);
      equals(cv1, cv2);
    }
  }

  private void equals(CacheValue cv1, CacheValue cv2) throws EvictionException {
    int l1 = cv1.length();
    int l2 = cv2.length();
    assertEquals(l1, l2);

    byte[] buf1 = new byte[l1];
    byte[] buf2 = new byte[l2];
    cv1.read(0, buf1, 0, l1);
    cv2.read(0, buf2, 0, l2);

    assertTrue(Arrays.equals(buf1, buf2));
  }

  private void populate(Random random, CacheValue cv1, CacheValue cv2) throws EvictionException {
    int l1 = cv1.length();
    int l2 = cv2.length();
    assertEquals(l1, l2);

    byte[] buf1 = new byte[l1];
    random.nextBytes(buf1);
    cv1.write(0, buf1, 0, l1);
    cv2.write(0, buf1, 0, l1);
    equals(cv1, cv2);
  }
}
