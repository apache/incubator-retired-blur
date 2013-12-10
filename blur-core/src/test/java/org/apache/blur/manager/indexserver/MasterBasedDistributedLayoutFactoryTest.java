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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.blur.MiniCluster;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class MasterBasedDistributedLayoutFactoryTest {

  private static String path = "./target/test-zk-MasterBasedDistributedLayoutFactoryTest";
  private static MiniCluster miniCluster;

  private ZooKeeper _zooKeeper;
  private String storagePath = "/MasterBasedDistributedLayoutFactoryTest";

  @BeforeClass
  public static void startZooKeeper() throws IOException {
    new File(path).mkdirs();
    miniCluster = new MiniCluster();
    miniCluster.startZooKeeper(path, true);
  }

  @AfterClass
  public static void stopZooKeeper() throws InterruptedException {
    miniCluster.shutdownZooKeeper();
  }

  @Before
  public void setup() throws IOException, KeeperException, InterruptedException {
    _zooKeeper = new ZooKeeper(miniCluster.getZkConnectionString(), 20000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
    rmr(_zooKeeper, storagePath);
  }

  @After
  public void teardown() throws InterruptedException {
    _zooKeeper.close();
  }

  @Test
  public void testDecreaseInServers() throws IOException, KeeperException, InterruptedException {
    MasterBasedDistributedLayoutFactory factory = new MasterBasedDistributedLayoutFactory(_zooKeeper, storagePath);

    List<String> shardList = list("shard-0", "shard-1", "shard-2", "shard-3", "shard-4", "shard-5");
    List<String> shardServerList = list("server-0", "server-1", "server-2", "server-3", "server-4", "server-5");
    List<String> offlineShardServers = list();

    String table = "t1";

    DistributedLayout layout1 = factory.createDistributedLayout(table, shardList, shardServerList, offlineShardServers,
        false);
    Map<String, String> expected1 = map(e("shard-0", "server-0"), e("shard-1", "server-1"), e("shard-2", "server-2"),
        e("shard-3", "server-3"), e("shard-4", "server-4"), e("shard-5", "server-5"));

    Map<String, String> actual1 = new TreeMap<String, String>(layout1.getLayout());

    assertEquals(expected1, actual1);

    List<String> newShardServerList = list("server-0", "server-1", "server-2", "server-3");
    List<String> newOfflineShardServers = list("server-4", "server-5");

    DistributedLayout layout2 = factory.createDistributedLayout(table, shardList, newShardServerList,
        newOfflineShardServers, false);

    Map<String, String> expected2 = map(e("shard-0", "server-0"), e("shard-1", "server-1"), e("shard-2", "server-2"),
        e("shard-3", "server-3"), e("shard-4", "server-0"), e("shard-5", "server-1"));
    Map<String, String> actual2 = new TreeMap<String, String>(layout2.getLayout());
    assertEquals(expected2, actual2);
  }

  @Test
  public void testIncreaseInServers() throws IOException, KeeperException, InterruptedException {
    MasterBasedDistributedLayoutFactory factory = new MasterBasedDistributedLayoutFactory(_zooKeeper, storagePath);

    List<String> shardList = list("shard-0", "shard-1", "shard-2", "shard-3", "shard-4", "shard-5");
    List<String> shardServerList = list("server-0", "server-1", "server-2", "server-3");
    List<String> offlineShardServers = list();

    String table = "t1";

    DistributedLayout layout1 = factory.createDistributedLayout(table, shardList, shardServerList, offlineShardServers,
        false);
    Map<String, String> expected1 = map(e("shard-0", "server-0"), e("shard-1", "server-1"), e("shard-2", "server-2"),
        e("shard-3", "server-3"), e("shard-4", "server-0"), e("shard-5", "server-1"));

    Map<String, String> actual1 = new TreeMap<String, String>(layout1.getLayout());

    assertEquals(expected1, actual1);

    List<String> newShardServerList = list("server-0", "server-1", "server-2", "server-3", "server-4", "server-5");
    List<String> newOfflineShardServers = list();

    DistributedLayout layout2 = factory.createDistributedLayout(table, shardList, newShardServerList,
        newOfflineShardServers, false);

    Map<String, String> expected2 = map(e("shard-0", "server-4"), e("shard-1", "server-5"), e("shard-2", "server-2"),
        e("shard-3", "server-3"), e("shard-4", "server-0"), e("shard-5", "server-1"));

    Map<String, String> actual2 = new TreeMap<String, String>(layout2.getLayout());
    assertEquals(expected2, actual2);
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

  private static Map<String, String> map(Entry<String, String>... entries) {
    Map<String, String> map = new TreeMap<String, String>();
    for (Entry<String, String> e : entries) {
      map.put(e.getKey(), e.getValue());
    }
    return map;
  }

  private static Entry<String, String> e(final String key, final String value) {
    return new Entry<String, String>() {

      @Override
      public String getKey() {
        return key;
      }

      @Override
      public String getValue() {
        return value;
      }

      @Override
      public String setValue(String value) {
        throw new RuntimeException("Not Supported");
      }

    };
  }
}
