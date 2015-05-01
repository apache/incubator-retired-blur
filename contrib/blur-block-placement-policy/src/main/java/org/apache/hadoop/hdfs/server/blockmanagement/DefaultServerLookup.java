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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DefaultServerLookup extends ServerLookup {

  public static final String BLUR_ZK_CONNECTIONS = "blur.zk.connections";

  private static final String SHARD_PATTERN = "/" + BlurConstants.SHARD_PREFIX;

  private static Log LOG = LogFactory.getLog(DefaultServerLookup.class);

  private final Host2NodesMap _host2datanodeMap;
  private final Map<String, Iface> _clients = new ConcurrentHashMap<String, Iface>();
  private final Map<Path, ShardTableInfo> _pathToShardMapping = new ConcurrentHashMap<Path, ShardTableInfo>();
  private final String _baseUri;
  private final List<Thread> _daemons = new ArrayList<Thread>();
  private final AtomicBoolean _running = new AtomicBoolean();
  private final long _pollTime;

  static class ShardTableInfo {
    final String tableName;
    final String shardServer;
    final String zkConnectionStr;

    ShardTableInfo(String tableName, String shardServer, String zkConnectionStr) {
      this.tableName = tableName;
      this.shardServer = shardServer;
      this.zkConnectionStr = zkConnectionStr;
    }

    @Override
    public String toString() {
      return "tableName=" + tableName + ", shardServer=" + shardServer + ", zkConnectionStr=" + zkConnectionStr + "";
    }
  }

  public DefaultServerLookup(Configuration conf, Host2NodesMap host2datanodeMap) {
    super(conf, host2datanodeMap);
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        _running.set(false);
      }
    }));
    _running.set(true);
    _pollTime = 1000;
    try {
      FileSystem fileSystem = FileSystem.get(conf);
      _baseUri = fileSystem.getUri().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    _host2datanodeMap = host2datanodeMap;
    String[] cons = conf.getStrings(BLUR_ZK_CONNECTIONS);
    if (cons != null) {
      for (String con : cons) {
        LOG.info("Blur client setup for [" + con + "]");
        _clients.put(con, BlurClient.getClientFromZooKeeperConnectionStr(con));
        startWatchDaemon(con);
        startCleanupDaemon(con);
      }
    }
  }

  private void startCleanupDaemon(final String con) {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          try {
            runLayoutCleanup(con);
          } catch (BlurException e) {
            LOG.error("Unknown error.", e);
          } catch (TException e) {
            LOG.error("Unknown error.", e);
          }
          try {
            Thread.sleep(_pollTime);
          } catch (InterruptedException e) {
            LOG.error("Unknown interruption.", e);
            return;
          }
        }
      }
    };
    Thread thread = new Thread(runnable);
    thread.setDaemon(true);
    thread.setName("Blur Cleanup for [" + con + "]");
    thread.start();
    _daemons.add(thread);
  }

  private void startWatchDaemon(final String con) {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          try {
            runLayoutUpdate(con);
          } catch (BlurException e) {
            LOG.error("Unknown error.", e);
          } catch (TException e) {
            LOG.error("Unknown error.", e);
          }
          try {
            Thread.sleep(_pollTime);
          } catch (InterruptedException e) {
            LOG.error("Unknown interruption.", e);
            return;
          }
        }
      }
    };
    Thread thread = new Thread(runnable);
    thread.setDaemon(true);
    thread.setName("Blur Watch for [" + con + "]");
    thread.start();
    _daemons.add(thread);
  }

  protected void runLayoutCleanup(String zkConnectionStr) throws BlurException, TException {
    Iface iface = _clients.get(zkConnectionStr);
    List<String> tableList = iface.tableList();
    Collection<String> enabledTables = new HashSet<String>();
    for (String table : tableList) {
      TableDescriptor tableDescriptor = iface.describe(table);
      String name = tableDescriptor.getName();
      if (tableDescriptor.isEnabled()) {
        enabledTables.add(name);
      }
    }
    cleanUp(enabledTables);
  }

  private void cleanUp(Collection<String> enabledTables) {
    Set<Entry<Path, ShardTableInfo>> entrySet = _pathToShardMapping.entrySet();
    Iterator<Entry<Path, ShardTableInfo>> iterator = entrySet.iterator();
    while (iterator.hasNext()) {
      Entry<Path, ShardTableInfo> entry = iterator.next();
      ShardTableInfo shardTableInfo = entry.getValue();
      String tableName = shardTableInfo.tableName;
      if (!enabledTables.contains(tableName)) {
        LOG.info("Removing ShardTableInfo [" + shardTableInfo + "]");
        iterator.remove();
      }
    }
  }

  private void runLayoutUpdate(String zkConnectionStr) throws BlurException, TException {
    Iface iface = _clients.get(zkConnectionStr);
    List<String> tableList = iface.tableList();
    for (String table : tableList) {
      TableDescriptor tableDescriptor = iface.describe(table);
      if (!tableDescriptor.isEnabled()) {
        continue;
      }
      String tableUri = tableDescriptor.getTableUri();
      Path tablePath = new Path(tableUri);
      String name = tableDescriptor.getName();
      Map<String, Map<String, ShardState>> shardServerLayoutState = iface.shardServerLayoutState(name);
      for (Entry<String, Map<String, ShardState>> entry : shardServerLayoutState.entrySet()) {
        String shardId = entry.getKey();
        Path shardPath = new Path(tablePath, shardId);
        Map<String, ShardState> serverStateMap = entry.getValue();
        String shardServer = getShardServer(serverStateMap);
        if (shardServer != null) {
          String shardServerHostName = getHostName(shardServer);
          ShardTableInfo shardTableInfo = new ShardTableInfo(name, shardServerHostName, zkConnectionStr);
          LOG.info("Adding mapping [" + shardPath + "] to ShardTableInfo [" + shardTableInfo + "]");
          _pathToShardMapping.put(shardPath, shardTableInfo);
        }
      }
    }
  }

  private String getHostName(String shardServer) {
    int indexOf = shardServer.indexOf(':');
    if (indexOf < 0) {
      return shardServer;
    }
    return shardServer.substring(0, indexOf);
  }

  private String getShardServer(Map<String, ShardState> serverStateMap) {
    for (Entry<String, ShardState> entry : serverStateMap.entrySet()) {
      String server = entry.getKey();
      ShardState shardState = entry.getValue();
      if (shardState == ShardState.OPEN) {
        return server;
      }
    }
    return null;
  }

  @Override
  public String getShardServer(String srcPath) {
    int indexOf = srcPath.indexOf(SHARD_PATTERN);
    if (indexOf < 0) {
      return null;
    }
    int end = srcPath.indexOf('/', indexOf + 1);
    if (end < 0) {
      return null;
    } else {
      Path shardPath = new Path(_baseUri + srcPath.substring(0, end));
      ShardTableInfo shardTableInfo = _pathToShardMapping.get(shardPath);
      LOG.info("Path [" + srcPath + "] Resolved to [" + shardPath.toString() + "] on ShardTableInfo [" + shardTableInfo
          + "]");
      if (shardTableInfo == null) {
        return null;
      }
      return shardTableInfo.shardServer;
    }
  }

  @Override
  public boolean isPathSupported(String srcPath) {
    if (!srcPath.contains(SHARD_PATTERN)) {
      return false;
    }
    return getShardServer(srcPath) != null;
  }

  @Override
  public DatanodeDescriptor getDatanodeDescriptor(String shardServer) {
    return _host2datanodeMap.getDataNodeByHostName(shardServer);
  }

}
