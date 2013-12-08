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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

public class MasterBasedLeveler {

  private static final Log LOG = LogFactory.getLog(MasterBasedLeveler.class);

  public static int level(int totalShards, int totalShardServers, Map<String, Integer> onlineServerShardCount,
      Map<String, String> newLayoutMap, String table, Random random) {
    List<Entry<String, Integer>> onlineServerShardCountList = new ArrayList<Map.Entry<String, Integer>>(
        onlineServerShardCount.entrySet());
    Collections.sort(onlineServerShardCountList, new Comparator<Entry<String, Integer>>() {
      @Override
      public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
        int value1 = o1.getValue();
        int value2 = o2.getValue();
        if (value1 == value2) {
          return 0;
        }
        return value1 < value2 ? -1 : 1;
      }
    });

    float opt = totalShards / (float) totalShardServers;

    Set<String> overAllocatedSet = new HashSet<String>();
    Set<String> underAllocatedSet = new HashSet<String>();
    LOG.debug("Optimum server shard count [{0}] for table [{1}]", opt, table);
    for (Entry<String, Integer> e : onlineServerShardCountList) {
      int countInt = e.getValue();
      float count = countInt;
      String server = e.getKey();
      if (isNotInOptBalance(opt, count)) {
        if (count > opt) {
          LOG.debug("Level server [{0}] over allocated at [{1}]", server, e.getValue());
          overAllocatedSet.add(server);
        } else {
          LOG.debug("Level server [{0}] under allocated at [{1}]", server, e.getValue());
          underAllocatedSet.add(server);
        }
      }
    }

    Map<String, SortedSet<String>> serverToShards = new HashMap<String, SortedSet<String>>();
    for (Entry<String, String> e : newLayoutMap.entrySet()) {
      String server = e.getValue();
      SortedSet<String> shards = serverToShards.get(server);
      if (shards == null) {
        shards = new TreeSet<String>();
        serverToShards.put(server, shards);
      }
      String shard = e.getKey();
      shards.add(shard);
    }

    int moves = 0;
    while (!underAllocatedSet.isEmpty() && !overAllocatedSet.isEmpty()) {
      String overAllocatedServer = getFirst(overAllocatedSet);
      String underAllocatedServer = getFirst(underAllocatedSet);
      LOG.debug("Over allocated server [{0}] under allocated server [{1}]", overAllocatedServer, underAllocatedServer);
      moveSingleShard(overAllocatedServer, underAllocatedServer, opt, overAllocatedSet, underAllocatedSet,
          newLayoutMap, onlineServerShardCount, serverToShards, table);
      moves++;
    }

    if (overAllocatedSet.size() > 0) {
      int count = (int) opt;
      LOG.debug("There are still [{0}] over allocated servers for table [{1}]", overAllocatedSet.size(), table);
      while (!overAllocatedSet.isEmpty()) {
        String overAllocatedServer = getFirst(overAllocatedSet);
        String underAllocatedServer = findServerWithCount(onlineServerShardCount, count, table, random);
        LOG.debug("Over allocated server [{0}] under allocated server [{1}]", overAllocatedServer, underAllocatedServer);
        moveSingleShard(overAllocatedServer, underAllocatedServer, opt, overAllocatedSet, underAllocatedSet,
            newLayoutMap, onlineServerShardCount, serverToShards, table);
        moves++;
      }
    }

    if (underAllocatedSet.size() > 0) {
      int count = (int) Math.ceil(opt);
      LOG.info("There are still [{0}] under allocated servers for table [{1}]", underAllocatedSet.size(), table);
      while (!underAllocatedSet.isEmpty()) {
        String overAllocatedServer = findServerWithCount(onlineServerShardCount, count, table, random);
        String underAllocatedServer = getFirst(underAllocatedSet);
        LOG.debug("Over allocated server [{0}] under allocated server [{1}]", overAllocatedServer, underAllocatedServer);
        moveSingleShard(overAllocatedServer, underAllocatedServer, opt, overAllocatedSet, underAllocatedSet,
            newLayoutMap, onlineServerShardCount, serverToShards, table);
        moves++;
      }
    }
    return moves;
  }

  private static String findServerWithCount(Map<String, Integer> onlineServerShardCount, int count, String table,
      Random random) {
    LOG.debug("Looking for server with shard count [{0}] for table [{1}]", count, table);
    List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(onlineServerShardCount.entrySet());
    Collections.shuffle(list, random);
    for (Entry<String, Integer> e : list) {
      int serverCount = e.getValue();
      if (serverCount == count) {
        return e.getKey();
      }
    }
    throw new RuntimeException("This should never happen");
  }

  private static boolean isNotInOptBalance(float opt, float count) {
    return Math.abs(count - opt) >= 1.0f;
  }

  private static String getFirst(Set<String> set) {
    return set.iterator().next();
  }

  private static void moveSingleShard(String srcServer, String distServer, float opt,
      Set<String> overAllocatedServerSet, Set<String> underAllocatedServerSet, Map<String, String> newLayoutMap,
      Map<String, Integer> onlineServerShardCount, Map<String, SortedSet<String>> serverToShards, String table) {

    SortedSet<String> srcShards = serverToShards.get(srcServer);
    if (srcShards == null) {
      srcShards = new TreeSet<String>();
      serverToShards.put(srcServer, srcShards);
    }
    SortedSet<String> distShards = serverToShards.get(distServer);
    if (distShards == null) {
      distShards = new TreeSet<String>();
      serverToShards.put(distServer, distShards);
    }

    LOG.debug("Source server shard list for table [{0}] is [{1}]", table, srcShards);
    LOG.debug("Destination server shard list for table [{0}] is [{1}]", table, distShards);

    String srcShard = getFirst(srcShards);

    LOG.debug("Moving shard [{0}] from [{1}] to [{2}] for table [{3}]", srcShard, srcServer, distServer, table);

    srcShards.remove(srcShard);
    distShards.add(srcShard);

    if (!isNotInOptBalance(opt, srcShards.size())) {
      LOG.debug("Source server [{0}] is in balance with size [{1}] optimum size [{2}]", srcServer, srcShards.size(),
          opt);
      overAllocatedServerSet.remove(srcServer);
      underAllocatedServerSet.remove(srcServer);
    }

    if (!isNotInOptBalance(opt, distShards.size())) {
      LOG.debug("Source server [{0}] is in balance with size [{1}] optimum size [{2}]", distServer, distShards.size(),
          opt);
      overAllocatedServerSet.remove(distServer);
      underAllocatedServerSet.remove(distServer);
    }

    newLayoutMap.put(srcShard, distServer);
    decr(onlineServerShardCount, srcServer);
    incr(onlineServerShardCount, distServer);
  }

  private static void incr(Map<String, Integer> map, String key) {
    Integer i = map.get(key);
    if (i == null) {
      map.put(key, 1);
    } else {
      map.put(key, i + 1);
    }
  }

  private static void decr(Map<String, Integer> map, String key) {
    Integer i = map.get(key);
    if (i == null) {
      map.put(key, 0);
    } else {
      int value = i - 1;
      if (value < 0) {
        value = 0;
      }
      map.put(key, value);
    }
  }
}
