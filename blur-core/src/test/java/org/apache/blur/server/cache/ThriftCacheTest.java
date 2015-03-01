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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.TableStats;
import org.junit.Test;

public class ThriftCacheTest {

  @Test
  public void test1() throws BlurException {
    ThriftCache thriftCache = new ThriftCache(10000);
    BlurQuery blurQuery = new BlurQuery();
    ThriftCacheKey<?> key = thriftCache.getKey("t", new int[] { 0, 1 }, blurQuery, BlurQuery.class);
    assertNull(thriftCache.get(key, BlurResults.class));
    BlurResults value = new BlurResults();
    // assert same instance
    assertTrue(value == thriftCache.put(key, value));
    BlurResults blurResults = thriftCache.get(key, BlurResults.class);
    assertFalse(value == blurResults);
    assertEquals(value, blurResults);
  }

  @Test
  public void test2() throws BlurException {
    ThriftCache thriftCache = new ThriftCache(10000);
    ThriftCacheKey<?> key = thriftCache.getKey("t", new int[] { 0, 1 }, null, TableStats.class);
    assertNull(thriftCache.get(key, TableStats.class));
    TableStats value = new TableStats();
    // assert same instance
    assertTrue(value == thriftCache.put(key, value));
    TableStats tableStats = thriftCache.get(key, TableStats.class);
    assertFalse(value == tableStats);
    assertEquals(value, tableStats);
  }

  @Test
  public void test3() throws BlurException {
    int maxSize = 100;
    ThriftCache thriftCache = new ThriftCache(maxSize);
    for (int i = 0; i < 1000000; i++) {
      BlurQuery blurQuery = new BlurQuery();
      // just make keys different
      blurQuery.fetch = i;
      ThriftCacheKey<?> key = thriftCache.getKey("t", new int[] { 0, 1 }, blurQuery, BlurQuery.class);
      assertNull(thriftCache.get(key, BlurResults.class));
      BlurResults value = new BlurResults();
      // assert same instance
      assertTrue(value == thriftCache.put(key, value));
      BlurResults blurResults = thriftCache.get(key, BlurResults.class);
      assertFalse(value == blurResults);
      assertEquals(value, blurResults);
    }
    long size = thriftCache.size();
    assertTrue(size <= maxSize);
  }

}
