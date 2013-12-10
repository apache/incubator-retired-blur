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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.blur.zookeeper.ZooKeeperLockManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class MasterBasedDistributedLayoutFactory implements DistributedLayoutFactory {

  private static final String TABLE_LAYOUT = "table_layout";
  private static final String LOCKS = "locks";
  private static final Log LOG = LogFactory.getLog(MasterBasedDistributedLayoutFactory.class);

  private final ConcurrentMap<String, MasterBasedDistributedLayout> _cachedLayoutMap = new ConcurrentHashMap<String, MasterBasedDistributedLayout>();
  private final ZooKeeper _zooKeeper;
  private final String _storagePath;
  private final ZooKeeperLockManager _zooKeeperLockManager;
  private final String _tableStoragePath;
  private final String _locksStoragePath;
  private final ThreadLocal<Random> _random = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  public MasterBasedDistributedLayoutFactory(ZooKeeper zooKeeper, String storagePath) {
    _zooKeeper = zooKeeper;
    _storagePath = storagePath;
    ZkUtils.mkNodes(_zooKeeper, _storagePath);
    ZkUtils.mkNodes(_zooKeeper, _storagePath, LOCKS);
    ZkUtils.mkNodes(_zooKeeper, _storagePath, TABLE_LAYOUT);
    _tableStoragePath = _storagePath + "/" + TABLE_LAYOUT;
    _locksStoragePath = _storagePath + "/" + LOCKS;
    _zooKeeperLockManager = new ZooKeeperLockManager(_zooKeeper, _locksStoragePath);
  }

  @Override
  public DistributedLayout createDistributedLayout(String table, List<String> shardList, List<String> shardServerList,
      List<String> offlineShardServers, boolean readOnly) {
    MasterBasedDistributedLayout layout = _cachedLayoutMap.get(table);
    List<String> onlineShardServerList = getOnlineShardServerList(shardServerList, offlineShardServers);
    if (layout == null || layout.isOutOfDate(shardList, onlineShardServerList)) {
      LOG.info("Layout out of date, recalculating for table [{0}].", table);
      MasterBasedDistributedLayout newLayout = newLayout(table, shardList, onlineShardServerList, readOnly);
      _cachedLayoutMap.put(table, newLayout);
      return newLayout;
    } else {
      return layout;
    }
  }

  private List<String> getOnlineShardServerList(List<String> shardServerList, List<String> offlineShardServers) {
    List<String> list = new ArrayList<String>(shardServerList);
    list.removeAll(offlineShardServers);
    return list;
  }

  private MasterBasedDistributedLayout newLayout(String table, List<String> onlineShardServerList,
      List<String> shardServerList, boolean readOnly) throws LayoutMissingException {
    try {
      _zooKeeperLockManager.lock(table);
      String storagePath = getStoragePath(table);
      LOG.info("Checking for existing layout for table [{0}]", table);
      Stat stat = _zooKeeper.exists(storagePath, false);
      MasterBasedDistributedLayout existingLayout = null;
      if (stat != null) {
        LOG.info("Existing layout found for table [{0}]", table);
        byte[] data = _zooKeeper.getData(storagePath, false, stat);
        if (data != null) {
          MasterBasedDistributedLayout storedLayout = fromBytes(data);
          LOG.info("Checking if layout is out of date for table [{0}]", table);
          if (!storedLayout.isOutOfDate(onlineShardServerList, shardServerList)) {
            LOG.info("Layout is up-to-date for table [{0}]", table);
            return storedLayout;
          }
          if (readOnly) {
            LOG.info("Using stable layout until update for table [{0}]", table);
            return storedLayout;
          }
          // If there was a stored layout, use the stored layout as a
          // replacement for the existing layout.
          existingLayout = storedLayout;
        }
      } else if (readOnly) {
        throw new LayoutMissingException();
      }
      LOG.info("Calculating new layout for table [{0}]", table);
      // recreate
      Map<String, String> newCalculatedLayout = calculateNewLayout(table, existingLayout, onlineShardServerList,
          shardServerList);
      MasterBasedDistributedLayout layout = new MasterBasedDistributedLayout(newCalculatedLayout,
          onlineShardServerList, shardServerList);
      LOG.info("New layout created for table [{0}]", table);
      if (_zooKeeper.exists(storagePath, false) == null) {
        _zooKeeper.create(storagePath, toBytes(layout), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else {
        _zooKeeper.setData(storagePath, toBytes(layout), -1);
      }
      return layout;
    } catch (LayoutMissingException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Unknown error during layout update.", e);
      throw new RuntimeException(e);
    } finally {
      try {
        _zooKeeperLockManager.unlock(table);
      } catch (InterruptedException e) {
        LOG.error("Unknown error during unlock.", e);
      } catch (KeeperException e) {
        LOG.error("Unknown error during unlock.", e);
      }
    }
  }

  private Map<String, String> calculateNewLayout(String table, MasterBasedDistributedLayout existingLayout,
      List<String> shardList, List<String> onlineShardServerList) {
    Set<String> shardServerSet = new TreeSet<String>(onlineShardServerList);
    if (shardServerSet.isEmpty()) {
      throw new RuntimeException("No online servers.");
    }
    if (existingLayout == null) {
      // blind setup, basic round robin
      LOG.info("Blind shard layout.");
      Map<String, String> newLayoutMap = new TreeMap<String, String>();
      Iterator<String> iterator = shardServerSet.iterator();
      for (String shard : shardList) {
        if (!iterator.hasNext()) {
          iterator = shardServerSet.iterator();
        }
        String server = iterator.next();
        newLayoutMap.put(shard, server);
      }
      return newLayoutMap;
    } else {
      LOG.info("Gather counts for table [{0}]", table);
      final Collection<String> shardsThatAreOffline = new TreeSet<String>();
      final Map<String, Integer> onlineServerShardCount = new TreeMap<String, Integer>();
      final Map<String, String> existingLayoutMap = existingLayout.getLayout();
      for (Entry<String, String> e : existingLayoutMap.entrySet()) {
        String shard = e.getKey();
        String server = e.getValue();
        if (!shardServerSet.contains(server)) {
          shardsThatAreOffline.add(shard);
        } else {
          increment(onlineServerShardCount, server);
        }
      }
      LOG.info("Existing layout counts for table [{0}] are [{1}] and offline shards are [{2}]", table,
          onlineServerShardCount, shardsThatAreOffline);

      LOG.info("Adding in new shard servers for table [{0}] current shard servers are [{1}]", table, shardServerSet);
      // Add counts for new shard servers
      for (String server : shardServerSet) {
        if (!onlineServerShardCount.containsKey(server)) {
          LOG.info("New shard server found [{0}]", server);
          onlineServerShardCount.put(server, 0);
        }
      }

      LOG.info("Assigning any missing shards [{1}] for table [{0}]", table, shardsThatAreOffline);
      // Assign missing shards
      final Map<String, String> newLayoutMap = new TreeMap<String, String>(existingLayoutMap);
      for (String offlineShard : shardsThatAreOffline) {
        // Find lowest shard count.
        String server = getServerWithTheLowest(onlineServerShardCount);
        LOG.info("Moving shard [{0}] to new server [{1}]", offlineShard, server);
        newLayoutMap.put(offlineShard, server);
        increment(onlineServerShardCount, server);
      }

      LOG.info("Leveling any shard hotspots for table [{0}] for layout [{1}]", table, newLayoutMap);
      // Level shards
      MasterBasedLeveler.level(shardList.size(), shardServerSet.size(), onlineServerShardCount, newLayoutMap, table,
          _random.get());
      return newLayoutMap;
    }
  }

  private static <K> void increment(Map<K, Integer> map, K k) {
    Integer count = map.get(k);
    if (count == null) {
      map.put(k, 1);
    } else {
      map.put(k, count + 1);
    }
  }

  private String getServerWithTheLowest(Map<String, Integer> onlineServerShardCount) {
    String server = null;
    int count = Integer.MAX_VALUE;
    for (Entry<String, Integer> e : onlineServerShardCount.entrySet()) {
      if (server == null || count > e.getValue()) {
        server = e.getKey();
        count = e.getValue();
      }
    }
    return server;
  }

  private byte[] toBytes(MasterBasedDistributedLayout layout) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    objectOutputStream.writeObject(layout);
    objectOutputStream.close();
    return byteArrayOutputStream.toByteArray();
  }

  private MasterBasedDistributedLayout fromBytes(byte[] data) throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
    ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
    try {
      return (MasterBasedDistributedLayout) objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      objectInputStream.close();
    }
  }

  private String getStoragePath(String table) {
    return _tableStoragePath + "/" + table;
  }

  @SuppressWarnings("serial")
  static class MasterBasedDistributedLayout implements DistributedLayout, Serializable {

    private final SortedSet<String> _shardList;
    private final SortedSet<String> _onlineShardServerList;
    private final Map<String, String> _layout;

    public MasterBasedDistributedLayout(Map<String, String> layout, Collection<String> shardList,
        Collection<String> onlineShardServerList) {
      _shardList = new TreeSet<String>(shardList);
      _onlineShardServerList = new TreeSet<String>(onlineShardServerList);
      _layout = layout;
    }

    @Override
    public Map<String, String> getLayout() {
      return _layout;
    }

    public boolean isOutOfDate(List<String> shardList, List<String> onlineShardServerList) {
      if (!_onlineShardServerList.equals(new TreeSet<String>(onlineShardServerList))) {
        return true;
      } else if (!_shardList.equals(new TreeSet<String>(shardList))) {
        return true;
      }
      return false;
    }
  }
}
