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
package org.apache.blur.manager.indexserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.blur.utils.ShardUtil;
import org.junit.Before;
import org.junit.Test;

public class MasterBasedLevelerTest {

  private static final String TABLE = "test";
  private static final int MAX_SERVERS = 10000;
  private static final int MAX_SHARDS = 100000;
  private long _seed;
  private Random _random;

  @Before
  public void setup() {
    _random = new Random();
    _seed = _random.nextLong();
//    _seed = 9095818143550884754L;
  }

  @Test
  public void testLeveler1() {
    try {
      Random random = new Random(_seed);

      int totalShardServers = atLeastOne(random.nextInt(MAX_SERVERS));
      int shards = atLeastOne(random.nextInt(MAX_SHARDS));

      Map<String, String> newLayoutMap = new TreeMap<String, String>();
      List<String> onlineServers = getOnlineServers(totalShardServers);
      populateCurrentLayout(random, newLayoutMap, shards, onlineServers);

      testLeveler(random, shards, newLayoutMap, onlineServers);

      // now test adding a server. no more that ceil(opt) should move
      onlineServers.add("newserver");
      {
        float opt = shards / (float) onlineServers.size();
        int moves = testLeveler(random, shards, newLayoutMap, onlineServers);
        assertTrue(moves <= Math.ceil(opt));
      }

      // now test removing a server. no more that ceil(opt) should move
      int index = random.nextInt(onlineServers.size());
      String serverToRemove = onlineServers.remove(index);
      System.out.println("removing [" + serverToRemove + "]");
      reassign(serverToRemove, newLayoutMap, onlineServers);
      {
        float opt = shards / (float) onlineServers.size();
        int moves = testLeveler(random, shards, newLayoutMap, onlineServers);
        assertTrue(moves <= Math.ceil(opt));
      }

    } catch (Throwable t) {
      t.printStackTrace();
      fail("Seed [" + _seed + "] exception");
    }
  }

  private int atLeastOne(int i) {
    if (i == 0) {
      return 1;
    }
    return i;
  }

  private void reassign(String serverToRemove, Map<String, String> newLayoutMap, List<String> onlineServers) {
    System.out.println("reassign - starting");
    Set<String> offlineShards = new HashSet<String>();
    for (Entry<String, String> entry : newLayoutMap.entrySet()) {
      if (entry.getValue().equals(serverToRemove)) {
        offlineShards.add(entry.getKey());
      }
    }
    Map<String, Integer> counts = populateCounts(newLayoutMap, onlineServers);
    counts.remove(serverToRemove);
    for (String offlineShard : offlineShards) {
      int count = Integer.MAX_VALUE;
      String server = null;
      for (Entry<String, Integer> e : counts.entrySet()) {
        if (e.getValue() < count) {
          count = e.getValue();
          server = e.getKey();
        }
      }
      if (server == null) {
        fail("Seed [" + _seed + "]");
      }
      newLayoutMap.put(offlineShard, server);
      Integer serverCount = counts.get(server);
      if (serverCount == null) {
        counts.put(server, 1);
      } else {
        counts.put(server, count + 1);
      }
    }
    System.out.println("reassign - ending");
  }

  private int testLeveler(Random random, int shards, Map<String, String> newLayoutMap, List<String> onlineServers) {
    int totalShardServers = onlineServers.size();
    float opt = shards / (float) totalShardServers;

    Map<String, Integer> beforeOnlineServerShardCount = populateCounts(newLayoutMap, onlineServers);
    int beforeLowCount = getLowCount(beforeOnlineServerShardCount);
    int beforeHighCount = getHighCount(beforeOnlineServerShardCount);
    System.out.println("Opt [" + opt + "] Before Low [" + beforeLowCount + "] High [" + beforeHighCount + "]");
    long s = System.nanoTime();
    int moves = MasterBasedLeveler.level(shards, totalShardServers, beforeOnlineServerShardCount, newLayoutMap, TABLE,
        random);
    long e = System.nanoTime();

    Map<String, Integer> afterOnlineServerShardCount = populateCounts(newLayoutMap, onlineServers);
    int afterLowCount = getLowCount(afterOnlineServerShardCount);
    int afterHighCount = getHighCount(afterOnlineServerShardCount);
    System.out.println("Opt [" + opt + "] After Low [" + afterLowCount + "] High [" + afterHighCount + "]");

    System.out.println("Total servers [" + totalShardServers + "] Total Shards [" + shards + "] Total moves [" + moves
        + "] in [" + (e - s) / 1000000.0 + " ms]");
    if (afterLowCount == afterHighCount) {
      assertEquals("Seed [" + _seed + "]", Math.round(opt), afterLowCount);
    } else if (afterLowCount + 1 == afterHighCount) {
      assertEquals("Seed [" + _seed + "]", (int) opt, afterLowCount);
    } else {
      fail("Seed [" + _seed + "]");
    }
    return moves;
  }

  @Test
  public void testLeveler2() {
    testLeveler1();
  }

  @Test
  public void testLeveler3() {
    testLeveler1();
  }

  @Test
  public void testLeveler4() {
    testLeveler1();
  }

  @Test
  public void testLeveler5() {
    testLeveler1();
  }

  @Test
  public void testLeveler6() {
    testLeveler1();
  }

  @Test
  public void testLeveler7() {
    testLeveler1();
  }

  @Test
  public void testLeveler8() {
    testLeveler1();
  }

  @Test
  public void testLeveler9() {
    testLeveler1();
  }

  @Test
  public void testLeveler10() {
    testLeveler1();
  }

  public void testLevelerALot() {
    for (int i = 0; i < 1000; i++) {
      _seed = _random.nextLong();
      testLeveler1();
    }
  }

  private List<String> getOnlineServers(int totalShardServers) {
    List<String> servers = new ArrayList<String>();
    for (int i = 0; i < totalShardServers; i++) {
      servers.add("server-" + i);
    }
    return servers;
  }

  private int getLowCount(Map<String, Integer> onlineServerShardCount) {
    int lowCount = Integer.MAX_VALUE;
    for (Entry<String, Integer> e : onlineServerShardCount.entrySet()) {
      int count = e.getValue();
      if (lowCount > count) {
        lowCount = count;
      }
    }
    return lowCount;
  }

  private int getHighCount(Map<String, Integer> onlineServerShardCount) {
    int highCount = Integer.MIN_VALUE;
    for (Entry<String, Integer> e : onlineServerShardCount.entrySet()) {
      int count = e.getValue();
      if (highCount < count) {
        highCount = count;
      }
    }
    return highCount;
  }

  private Map<String, Integer> populateCounts(Map<String, String> newLayoutMap, List<String> servers) {
    Map<String, Integer> onlineServerShardCount = new TreeMap<String, Integer>();
    for (String server : servers) {
      onlineServerShardCount.put(server, 0);
    }
    for (Entry<String, String> e : newLayoutMap.entrySet()) {
      String value = e.getValue();
      Integer count = onlineServerShardCount.get(value);
      if (count == null) {
        onlineServerShardCount.put(value, 1);
      } else {
        onlineServerShardCount.put(value, count + 1);
      }
    }
    return onlineServerShardCount;
  }

  private void populateCurrentLayout(Random random, Map<String, String> newLayoutMap, int shards, List<String> servers) {
    for (int i = 0; i < shards; i++) {
      String shardName = ShardUtil.getShardName(i);
      int server = random.nextInt(servers.size());
      newLayoutMap.put(shardName, servers.get(server));
    }
  }

}
