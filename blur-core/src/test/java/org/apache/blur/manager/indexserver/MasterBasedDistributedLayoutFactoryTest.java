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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class MasterBasedDistributedLayoutFactoryTest {

  private ZooKeeper _zooKeeper;
  private String storagePath = "/proto_layout";

  @Before
  public void setup() throws IOException, KeeperException, InterruptedException {
    _zooKeeper = new ZooKeeper("127.0.0.1", 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
    rmr(_zooKeeper, storagePath);
  }

  @Test
  public void testDecreaseInServers() throws IOException, KeeperException, InterruptedException {
    MasterBasedDistributedLayoutFactory factory = new MasterBasedDistributedLayoutFactory(_zooKeeper, storagePath);

    List<String> shardList = list("shard-0", "shard-1", "shard-2", "shard-3", "shard-4", "shard-5");
    List<String> shardServerList = list("server-0", "server-1", "server-2", "server-3", "server-4", "server-5");
    List<String> offlineShardServers = list();

    String table = "t1";

    DistributedLayout layout1 = factory.createDistributedLayout(table, shardList, shardServerList, offlineShardServers);

    Map<String, String> map1 = new TreeMap<String, String>(layout1.getLayout());
    for (Entry<String, String> e : map1.entrySet()) {
      System.out.println(e.getKey() + " " + e.getValue());
    }

    List<String> newShardServerList = list("server-0", "server-1", "server-2", "server-3");
    List<String> newOfflineShardServers = list("server-4", "server-5");

    DistributedLayout layout2 = factory.createDistributedLayout(table, shardList, newShardServerList,
        newOfflineShardServers);
    System.out.println("================");
    Map<String, String> map2 = new TreeMap<String, String>(layout2.getLayout());
    for (Entry<String, String> e : map2.entrySet()) {
      System.out.println(e.getKey() + " " + e.getValue());
    }
  }

  @Test
  public void testIncreaseInServers() throws IOException, KeeperException, InterruptedException {
    MasterBasedDistributedLayoutFactory factory = new MasterBasedDistributedLayoutFactory(_zooKeeper, storagePath);

    List<String> shardList = list("shard-0", "shard-1", "shard-2", "shard-3", "shard-4", "shard-5");
    List<String> shardServerList = list("server-0", "server-1", "server-2", "server-3");
    List<String> offlineShardServers = list();

    String table = "t1";

    DistributedLayout layout1 = factory.createDistributedLayout(table, shardList, shardServerList, offlineShardServers);

    Map<String, String> map1 = new TreeMap<String, String>(layout1.getLayout());
    for (Entry<String, String> e : map1.entrySet()) {
      System.out.println(e.getKey() + " " + e.getValue());
    }

    List<String> newShardServerList = list("server-0", "server-1", "server-2", "server-3", "server-4", "server-5");
    List<String> newOfflineShardServers = list();

    DistributedLayout layout2 = factory.createDistributedLayout(table, shardList, newShardServerList,
        newOfflineShardServers);
    System.out.println("================");
    Map<String, String> map2 = new TreeMap<String, String>(layout2.getLayout());
    for (Entry<String, String> e : map2.entrySet()) {
      System.out.println(e.getKey() + " " + e.getValue());
    }
  }

  private void rmr(ZooKeeper zooKeeper, String storagePath) throws KeeperException, InterruptedException {
    Stat stat = zooKeeper.exists(storagePath, false);
    if (stat == null) {
      return;
    }
    List<String> children = zooKeeper.getChildren(storagePath, false);
    for (String s : children) {
      rmr(zooKeeper, storagePath + "/" + s);
    }
    zooKeeper.delete(storagePath, -1);
  }

  private static List<String> list(String... list) {
    List<String> lst = new ArrayList<String>();
    for (String s : list) {
      lst.add(s);
    }
    return lst;
  }
}
