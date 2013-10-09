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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

public class MasterBasedLeveler {

  private static final Log LOG = LogFactory.getLog(MasterBasedLeveler.class);

  public static void level(int totalShards, int totalShardServers, Map<String, Integer> onlineServerShardCount,
      Map<String, String> newLayoutMap) {
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
    LOG.info("Optimum server shard count [{0}]", opt);
    for (Entry<String, Integer> e : onlineServerShardCountList) {
      int countInt = e.getValue();
      float count = countInt;
      String server = e.getKey();
      if (isNotInOptBalance(opt, count)) {
        if (count > opt) {
          LOG.info("Level server [{0}] over allocated at [{1}]", server, e.getValue());
          overAllocatedSet.add(server);
        } else {
          LOG.info("Level server [{0}] under allocated at [{1}]", server, e.getValue());
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

    while (!underAllocatedSet.isEmpty() && !overAllocatedSet.isEmpty()) {
      String overAllocatedServer = getFirst(overAllocatedSet);
      String underAllocatedServer = getFirst(underAllocatedSet);
      moveSingleShard(overAllocatedServer, underAllocatedServer, opt, overAllocatedSet, underAllocatedSet,
          newLayoutMap, onlineServerShardCount, serverToShards);
    }
  }

  private static boolean isNotInOptBalance(float opt, float count) {
    return Math.abs(count - opt) >= 1.0f;
  }

  private static String getFirst(Set<String> set) {
    return set.iterator().next();
  }

  private static void moveSingleShard(String srcServer, String distServer, float opt,
      Set<String> overAllocatedServerSet, Set<String> underAllocatedServerSet, Map<String, String> newLayoutMap,
      Map<String, Integer> onlineServerShardCount, Map<String, SortedSet<String>> serverToShards) {

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

    String srcShard = getFirst(srcShards);

    srcShards.remove(srcShard);
    distShards.add(srcShard);

    if (!isNotInOptBalance(opt, srcShards.size())) {
      overAllocatedServerSet.remove(srcServer);
      underAllocatedServerSet.remove(srcServer);
    }

    if (!isNotInOptBalance(opt, distShards.size())) {
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
