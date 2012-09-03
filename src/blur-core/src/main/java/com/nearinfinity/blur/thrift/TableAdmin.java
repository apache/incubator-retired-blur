package com.nearinfinity.blur.thrift;

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
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.clusterstatus.ClusterStatus;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.TableStats;

public abstract class TableAdmin implements Iface {

  private static final Log LOG = LogFactory.getLog(TableAdmin.class);
  protected ZooKeeper _zookeeper;
  protected ClusterStatus _clusterStatus;
  protected BlurConfiguration _configuration;

  @Override
  public TableStats getTableStats(String table) throws BlurException, TException {
    return tableStats(table);
  }

  @Override
  public boolean isInSafeMode(String cluster) throws BlurException, TException {
    try {
      return _clusterStatus.isInSafeMode(true, cluster);
    } catch (Exception e) {
      LOG.error("Unknown error during safe mode check of [cluster={0}]", e, cluster);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public final void createTable(TableDescriptor tableDescriptor) throws BlurException, TException {
    try {
      // @todo Remove this once issue #27 is resolved
      if (tableDescriptor.compressionBlockSize > 32768) {
        tableDescriptor.compressionBlockSize = 32768;
      } else if (tableDescriptor.compressionBlockSize < 8192) {
        tableDescriptor.compressionBlockSize = 8192;
      }
      _clusterStatus.createTable(tableDescriptor);
    } catch (Exception e) {
      LOG.error("Unknown error during create of [table={0}, tableDescriptor={1}]", e, tableDescriptor.name, tableDescriptor);
      throw new BException(e.getMessage(), e);
    }
    if (tableDescriptor.isEnabled) {
      enableTable(tableDescriptor.name);
    }
  }

  @Override
  public final void disableTable(String table) throws BlurException, TException {
    try {
      String cluster = _clusterStatus.getCluster(false, table);
      if (cluster == null) {
        throw new BlurException("Table [" + table + "] not found.", null);
      }
      _clusterStatus.disableTable(cluster, table);
      waitForTheTableToDisable(cluster, table);
      waitForTheTableToDisengage(cluster, table);
    } catch (Exception e) {
      LOG.error("Unknown error during disable of [table={0}]", e, table);
      throw new BException(e.getMessage(), e);
    }
  }

  private void waitForTheTableToDisengage(String cluster, String table) throws BlurException, TException {
    // LOG.info("Waiting for shards to disengage on table [" + table + "]");
  }

  private void waitForTheTableToDisable(String cluster, String table) throws BlurException, TException {
    LOG.info("Waiting for shards to disable on table [" + table + "]");
    while (true) {
      if (!_clusterStatus.isEnabled(false, cluster, table)) {
        return;
      }
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        LOG.error("Unknown error while enabling table [" + table + "]", e);
        throw new BException("Unknown error while enabling table [" + table + "]", e);
      }
    }
  }

  @Override
  public final void enableTable(String table) throws BlurException, TException {
    try {
      String cluster = _clusterStatus.getCluster(false, table);
      if (cluster == null) {
        throw new BlurException("Table [" + table + "] not found.", null);
      }
      _clusterStatus.enableTable(cluster, table);
      waitForTheTableToEnable(cluster, table);
      waitForTheTableToEngage(cluster, table);
    } catch (Exception e) {
      LOG.error("Unknown error during enable of [table={0}]", e, table);
      throw new BException(e.getMessage(), e);
    }
  }

  private void waitForTheTableToEnable(String cluster, String table) throws BlurException {
    LOG.info("Waiting for shards to engage on table [" + table + "]");
    while (true) {
      if (_clusterStatus.isEnabled(false, cluster, table)) {
        return;
      }
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        LOG.error("Unknown error while enabling table [" + table + "]", e);
        throw new BException("Unknown error while enabling table [" + table + "]", e);
      }
    }
  }

  private void waitForTheTableToEngage(String cluster, String table) throws BlurException, TException {
    TableDescriptor describe = describe(table);
    int shardCount = describe.shardCount;
    LOG.info("Waiting for shards to engage on table [" + table + "]");
    while (true) {
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        LOG.error("Unknown error while engaging table [" + table + "]", e);
        throw new BException("Unknown error while engaging table [" + table + "]", e);
      }
      try {
        Map<String, String> shardServerLayout = shardServerLayout(table);
        LOG.info("Shards [" + shardServerLayout.size() + "/" + shardCount + "] of table [" + table + "] engaged");
        if (shardServerLayout.size() == shardCount) {
          return;
        }
      } catch (BlurException e) {
        LOG.info("Stilling waiting", e);
      } catch (TException e) {
        LOG.info("Stilling waiting", e);
      }
    }
  }

  @Override
  public final void removeTable(String table, boolean deleteIndexFiles) throws BlurException, TException {
    try {
      String cluster = _clusterStatus.getCluster(false, table);
      if (cluster == null) {
        throw new BlurException("Table [" + table + "] not found.", null);
      }
      _clusterStatus.removeTable(cluster, table, deleteIndexFiles);
    } catch (Exception e) {
      LOG.error("Unknown error during remove of [table={0}]", e, table);
      throw new BException(e.getMessage(), e);
    }
  }

  public boolean isTableEnabled(boolean useCache, String cluster, String table) {
    return _clusterStatus.isEnabled(useCache, cluster, table);
  }

  public void checkTable(String table) throws BlurException {
    if (table == null) {
      throw new BlurException("Table cannot be null.", null);
    }
    String cluster = _clusterStatus.getCluster(true, table);
    if (cluster == null) {
      throw new BlurException("Table [" + table + "] does not exist", null);
    }
    checkTable(cluster, table);
  }

  public void checkTable(String cluster, String table) throws BlurException {
    if (inSafeMode(true, table)) {
      throw new BlurException("Cluster for [" + table + "] is in safe mode", null);
    }
    if (tableExists(true, cluster, table)) {
      if (isTableEnabled(true, cluster, table)) {
        return;
      }
      throw new BlurException("Table [" + table + "] exists, but is not enabled", null);
    } else {
      throw new BlurException("Table [" + table + "] does not exist", null);
    }
  }

  public void checkForUpdates(String table) throws BlurException {
    String cluster = _clusterStatus.getCluster(true, table);
    if (cluster == null) {
      throw new BlurException("Table [" + table + "] does not exist", null);
    }
    checkForUpdates(cluster, table);
  }

  public void checkForUpdates(String cluster, String table) throws BlurException {
    if (_clusterStatus.isReadOnly(true, cluster, table)) {
      throw new BlurException("Table [" + table + "] in cluster [" + cluster + "] is read only.", null);
    }
  }

  @Override
  public final List<String> controllerServerList() throws BlurException, TException {
    try {
      return _clusterStatus.getControllerServerList();
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get a controller list.", e);
      throw new BException("Unknown error while trying to get a controller list.", e);
    }
  }

  @Override
  public final List<String> shardServerList(String cluster) throws BlurException, TException {
    try {
      return _clusterStatus.getShardServerList(cluster);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get a shard server list.", e);
      throw new BException("Unknown error while trying to get a shard server list.", e);
    }
  }

  @Override
  public final List<String> shardClusterList() throws BlurException, TException {
    try {
      return _clusterStatus.getClusterList(true);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get a cluster list.", e);
      throw new BException("Unknown error while trying to get a cluster list.", e);
    }
  }

  @Override
  public final TableDescriptor describe(final String table) throws BlurException, TException {
    try {
      String cluster = _clusterStatus.getCluster(true, table);
      if (cluster == null) {
        throw new BlurException("Table [" + table + "] not found.", null);
      }
      return _clusterStatus.getTableDescriptor(true, cluster, table);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to describe a table [" + table + "].", e);
      throw new BException("Unknown error while trying to describe a table [" + table + "].", e);
    }
  }

  @Override
  public final List<String> tableListByCluster(String cluster) throws BlurException, TException {
    try {
      return _clusterStatus.getTableList(true, cluster);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get a table list by cluster [" + cluster + "].", e);
      throw new BException("Unknown error while trying to get a table list by cluster [" + cluster + "].", e);
    }
  }

  @Override
  public final List<String> tableList() throws BlurException, TException {
    try {
      return _clusterStatus.getTableList(true);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get a table list.", e);
      throw new BException("Unknown error while trying to get a table list.", e);
    }
  }

  private boolean inSafeMode(boolean useCache, String table) throws BlurException {
    String cluster = _clusterStatus.getCluster(useCache, table);
    if (cluster == null) {
      throw new BlurException("Table [" + table + "] not found.", null);
    }
    return _clusterStatus.isInSafeMode(useCache, cluster);
  }

  public boolean tableExists(boolean useCache, String cluster, String table) {
    return _clusterStatus.exists(useCache, cluster, table);
  }

  public ClusterStatus getClusterStatus() {
    return _clusterStatus;
  }

  public void setClusterStatus(ClusterStatus clusterStatus) {
    _clusterStatus = clusterStatus;
  }

  public void setZookeeper(ZooKeeper zookeeper) {
    _zookeeper = zookeeper;
  }

  @Override
  public Map<String, String> configuration() throws BlurException, TException {
    return _configuration.getProperties();
  }
}
