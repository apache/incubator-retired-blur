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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.user.User;
import org.junit.Test;

public class ThriftCacheKeyTest {

  @Test
  public void test1() throws BlurException {
    User user = null;
    String table = "t";
    BlurQuery bq1 = new BlurQuery();
    BlurQuery bq2 = new BlurQuery();
    ThriftCacheKey<BlurQuery> key1 = new ThriftCacheKey<BlurQuery>(user, table, new int[] { 0, 1 }, bq1,
        BlurQuery.class);
    ThriftCacheKey<BlurQuery> key2 = new ThriftCacheKey<BlurQuery>(user, table, new int[] { 0, 1 }, bq2,
        BlurQuery.class);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void test2() throws BlurException {
    User user = null;
    String table = "t";
    ThriftCacheKey<BlurQuery> key1 = new ThriftCacheKey<BlurQuery>(user, table, new int[] { 0, 1 }, null,
        BlurQuery.class);
    ThriftCacheKey<BlurQuery> key2 = new ThriftCacheKey<BlurQuery>(user, table, new int[] { 0, 1 }, null,
        BlurQuery.class);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void test3() throws BlurException {
    User user = new User("test", null);
    String table = "t";
    BlurQuery bq1 = new BlurQuery();
    BlurQuery bq2 = new BlurQuery();
    ThriftCacheKey<BlurQuery> key1 = new ThriftCacheKey<BlurQuery>(user, table, new int[] { 0, 1 }, bq1,
        BlurQuery.class);
    ThriftCacheKey<BlurQuery> key2 = new ThriftCacheKey<BlurQuery>(user, table, new int[] { 0, 1 }, bq2,
        BlurQuery.class);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void test4() throws BlurException {
    User user = new User("test", map("a", "b"));
    String table = "t";
    BlurQuery bq1 = new BlurQuery();
    BlurQuery bq2 = new BlurQuery();
    ThriftCacheKey<BlurQuery> key1 = new ThriftCacheKey<BlurQuery>(user, table, new int[] { 0, 1 }, bq1,
        BlurQuery.class);
    ThriftCacheKey<BlurQuery> key2 = new ThriftCacheKey<BlurQuery>(user, table, new int[] { 0, 1 }, bq2,
        BlurQuery.class);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void test5a() throws BlurException {
    User user1 = new User("test1", null);
    User user2 = new User("test2", null);
    String table = "t";
    BlurQuery bq1 = new BlurQuery();
    BlurQuery bq2 = new BlurQuery();
    ThriftCacheKey<BlurQuery> key1 = new ThriftCacheKey<BlurQuery>(user1, table, new int[] { 0, 1 }, bq1,
        BlurQuery.class);
    ThriftCacheKey<BlurQuery> key2 = new ThriftCacheKey<BlurQuery>(user2, table, new int[] { 0, 1 }, bq2,
        BlurQuery.class);
    assertTrue(key1.equals(key2));
    assertTrue(key1.hashCode() == key2.hashCode());
  }

  @Test
  public void test5b() throws BlurException {
    User user1 = new User("test1", null);
    User user2 = new User("test1", null);
    String table = "t";
    BlurQuery bq1 = new BlurQuery();
    BlurQuery bq2 = new BlurQuery();
    ThriftCacheKey<BlurQuery> key1 = new ThriftCacheKey<BlurQuery>(user1, table, new int[] { 0, 1 }, bq1,
        BlurQuery.class);
    ThriftCacheKey<BlurQuery> key2 = new ThriftCacheKey<BlurQuery>(user2, table, new int[] { 0, 2 }, bq2,
        BlurQuery.class);

    assertFalse(key1.equals(key2));
    assertFalse(key1.hashCode() == key2.hashCode());
  }

  @Test
  public void test6() throws BlurException {
    User user1 = new User("test1", map("a", "b"));
    User user2 = new User("test1", map("a", "c"));
    String table = "t";
    BlurQuery bq1 = new BlurQuery();
    BlurQuery bq2 = new BlurQuery();
    ThriftCacheKey<BlurQuery> key1 = new ThriftCacheKey<BlurQuery>(user1, table, new int[] { 0, 1 }, bq1,
        BlurQuery.class);
    ThriftCacheKey<BlurQuery> key2 = new ThriftCacheKey<BlurQuery>(user2, table, new int[] { 0, 1 }, bq2,
        BlurQuery.class);

    assertFalse(key1.equals(key2));
    assertFalse(key1.hashCode() == key2.hashCode());
  }

  @Test
  public void test7() throws BlurException {
    User user1 = new User("test1", map("a", "b"));
    User user2 = new User("test2", map("a", "b"));
    String table = "t";
    BlurQuery bq1 = new BlurQuery();
    BlurQuery bq2 = new BlurQuery();
    ThriftCacheKey<BlurQuery> key1 = new ThriftCacheKey<BlurQuery>(user1, table, new int[] { 0, 1 }, bq1,
        BlurQuery.class);
    ThriftCacheKey<BlurQuery> key2 = new ThriftCacheKey<BlurQuery>(user2, table, new int[] { 0, 1 }, bq2,
        BlurQuery.class);

    assertTrue(key1.equals(key2));
    assertTrue(key1.hashCode() == key2.hashCode());
  }

  private Map<String, String> map(String... s) {
    if (s == null) {
      return null;
    }
    if (s.length % 2 != 0) {
      throw new RuntimeException("Can only take pairs.");
    }
    Map<String, String> map = new HashMap<String, String>();
    for (int i = 1; i < s.length; i += 2) {
      map.put(s[i - 1], s[i]);
    }
    return map;
  }

  public static SortedSet<String> getShards(String... shards) {
    SortedSet<String> set = new TreeSet<String>();
    for (String s : shards) {
      set.add(s);
    }
    return set;
  }
}
