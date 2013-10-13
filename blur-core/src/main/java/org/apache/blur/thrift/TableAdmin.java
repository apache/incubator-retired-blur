package org.apache.blur.thrift;

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
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.analysis.FieldManager;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.clusterstatus.ClusterStatus;
import org.apache.blur.server.TableContext;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Metric;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.MemoryReporter;
import org.apache.zookeeper.ZooKeeper;

public abstract class TableAdmin implements Iface {

  private static final Log LOG = LogFactory.getLog(TableAdmin.class);
  protected ZooKeeper _zookeeper;
  protected ClusterStatus _clusterStatus;
  protected BlurConfiguration _configuration;
  protected int _maxRecordsPerRowFetchRequest = 1000;

  protected void checkSelectorFetchSize(Selector selector) {
    if (selector == null) {
      return;
    }
    int maxRecordsToFetch = selector.getMaxRecordsToFetch();
    if (maxRecordsToFetch > _maxRecordsPerRowFetchRequest) {
      LOG.warn("Max records to fetch is too high [{0}] max [{1}] in Selector [{2}]", maxRecordsToFetch,
          _maxRecordsPerRowFetchRequest, selector);
      selector.setMaxRecordsToFetch(_maxRecordsPerRowFetchRequest);
    }
  }

  @Override
  public Map<String, Metric> metrics(Set<String> metrics) throws BlurException, TException {
    try {
      Map<String, Metric> metricsMap = MemoryReporter.getMetrics();
      if (metrics == null) {
        return metricsMap;
      } else {
        Map<String, Metric> result = new HashMap<String, Metric>();
        for (String n : metrics) {
          Metric metric = metricsMap.get(n);
          if (metric != null) {
            result.put(n, metric);
          }
        }
        return result;
      }
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get metrics [{0}] ", e, metrics);
      throw new BException(e.getMessage(), e);
    }
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
      TableContext.clear();
      BlurUtil.validateTableName(tableDescriptor.getName());
      assignClusterIfNull(tableDescriptor);
      _clusterStatus.createTable(tableDescriptor);
    } catch (Exception e) {
      LOG.error("Unknown error during create of [table={0}, tableDescriptor={1}]", e, tableDescriptor.name,
          tableDescriptor);
      throw new BException(e.getMessage(), e);
    }
    if (tableDescriptor.isEnabled()) {
      enableTable(tableDescriptor.getName());
    }
  }

  private void assignClusterIfNull(TableDescriptor tableDescriptor) throws BlurException, TException {
    if (tableDescriptor.getCluster() == null) {
      List<String> shardClusterList = shardClusterList();
      if (shardClusterList != null && shardClusterList.size() == 1) {
        String cluster = shardClusterList.get(0);
        tableDescriptor.setCluster(cluster);
        LOG.info("Assigning table [{0}] to the single default cluster [{1}]", tableDescriptor.getName(), cluster);
      }
    }
  }

  @Override
  public final void disableTable(String table) throws BlurException, TException {
    try {
      TableContext.clear();
      String cluster = _clusterStatus.getCluster(false, table);
      if (cluster == null) {
        throw new BException("Table [" + table + "] not found.");
      }
      _clusterStatus.disableTable(cluster, table);
      waitForTheTableToDisable(cluster, table);
      waitForTheTableToDisengage(cluster, table);
    } catch (Exception e) {
      LOG.error("Unknown error during disable of [table={0}]", e, table);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public final void enableTable(String table) throws BlurException, TException {
    try {
      TableContext.clear();
      String cluster = _clusterStatus.getCluster(false, table);
      if (cluster == null) {
        throw new BException("Table [" + table + "] not found.");
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

  /**
   * This method only works on controllers, if called on shard servers it will
   * only wait itself to finish not the whole cluster.
   */
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
        Map<String, Map<String, ShardState>> shardServerLayoutState = shardServerLayoutState(table);

        int countNumberOfOpen = 0;
        int countNumberOfOpening = 0;
        for (Entry<String, Map<String, ShardState>> shardEntry : shardServerLayoutState.entrySet()) {
          Map<String, ShardState> value = shardEntry.getValue();
          for (ShardState state : value.values()) {
            if (state == ShardState.OPEN) {
              countNumberOfOpen++;
            } else if (state == ShardState.OPENING) {
              countNumberOfOpening++;
            } else {
              LOG.warn("Unexpected state of [{0}] for shard [{1}].", state, shardEntry.getKey());
            }
          }
        }
        LOG.info("Opening - Shards Open [{0}], Shards Opening [{1}] of table [{2}]", countNumberOfOpen,
            countNumberOfOpening, table);
        if (countNumberOfOpen == shardCount && countNumberOfOpening == 0) {
          return;
        }
      } catch (BlurException e) {
        LOG.info("Stilling waiting", e);
      } catch (TException e) {
        LOG.info("Stilling waiting", e);
      }
    }
  }

  /**
   * This method only works on controllers, if called on shard servers it will
   * only wait itself to finish not the whole cluster.
   */
  private void waitForTheTableToDisengage(String cluster, String table) throws BlurException, TException {
    LOG.info("Waiting for shards to disengage on table [" + table + "]");
    while (true) {
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        LOG.error("Unknown error while disengaging table [" + table + "]", e);
        throw new BException("Unknown error while disengaging table [" + table + "]", e);
      }
      try {
        Map<String, Map<String, ShardState>> shardServerLayoutState = shardServerLayoutState(table);

        int countNumberOfOpen = 0;
        int countNumberOfClosing = 0;
        for (Entry<String, Map<String, ShardState>> shardEntry : shardServerLayoutState.entrySet()) {
          Map<String, ShardState> value = shardEntry.getValue();
          for (ShardState state : value.values()) {
            if (state == ShardState.OPEN) {
              countNumberOfOpen++;
            } else if (state == ShardState.CLOSING) {
              countNumberOfClosing++;
            } else if (state == ShardState.CLOSED) {
              LOG.info("Shard [{0}] of table [{1}] now reporting closed.", shardEntry.getKey(), table);
            } else {
              LOG.warn("Unexpected state of [{0}] for shard [{1}].", state, shardEntry.getKey());
            }
          }
        }
        LOG.info("Closing - Shards Open [{0}], Shards Closing [{1}] of table [{2}]", countNumberOfOpen,
            countNumberOfClosing, table);
        if (countNumberOfOpen == 0 && countNumberOfClosing == 0) {
          return;
        }
      } catch (BlurException e) {
        LOG.info("Stilling waiting", e);
      } catch (TException e) {
        LOG.info("Stilling waiting", e);
      }
    }
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
  public final void removeTable(String table, boolean deleteIndexFiles) throws BlurException, TException {
    try {
      TableContext.clear();
      String cluster = _clusterStatus.getCluster(false, table);
      if (cluster == null) {
        throw new BException("Table [" + table + "] not found.");
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
      throw new BException("Table cannot be null.");
    }
    String cluster = _clusterStatus.getCluster(true, table);
    if (cluster == null) {
      throw new BException("Table [" + table + "] does not exist");
    }
    checkTable(cluster, table);
  }

  public void checkTable(String cluster, String table) throws BlurException {
    if (inSafeMode(true, table)) {
      throw new BException("Cluster for [" + table + "] is in safe mode");
    }
    if (tableExists(true, cluster, table)) {
      if (isTableEnabled(true, cluster, table)) {
        return;
      }
      throw new BException("Table [" + table + "] exists, but is not enabled");
    } else {
      throw new BException("Table [" + table + "] does not exist");
    }
  }

  public void checkForUpdates(String table) throws BlurException {
    String cluster = _clusterStatus.getCluster(true, table);
    if (cluster == null) {
      throw new BException("Table [" + table + "] does not exist");
    }
    checkForUpdates(cluster, table);
  }

  public void checkForUpdates(String cluster, String table) throws BlurException {
    if (_clusterStatus.isReadOnly(true, cluster, table)) {
      throw new BException("Table [" + table + "] in cluster [" + cluster + "] is read only.");
    }
  }

  @Override
  public final List<String> controllerServerList() throws BlurException, TException {
    try {
      return _clusterStatus.getOnlineControllerList();
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
        throw new BException("Table [" + table + "] not found.");
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

  @Override
  public boolean addColumnDefinition(String table, ColumnDefinition columnDefinition) throws BlurException, TException {
    if (table == null) {
      throw new BException("Table cannot be null.");
    }
    if (columnDefinition == null) {
      throw new BException("ColumnDefinition cannot be null.");
    }
    TableDescriptor tableDescriptor = describe(table);
    TableContext context = TableContext.create(tableDescriptor);
    FieldManager fieldManager = context.getFieldManager();
    String family = columnDefinition.getFamily();
    if (family == null) {
      throw new BException("Family in ColumnDefinition [{0}] cannot be null.", columnDefinition);
    }
    String columnName = columnDefinition.getColumnName();
    if (columnName == null) {
      throw new BException("ColumnName in ColumnDefinition [{0}] cannot be null.", columnDefinition);
    }
    String subColumnName = columnDefinition.getSubColumnName();
    boolean fieldLessIndexed = columnDefinition.isFieldLessIndexed();
    String fieldType = columnDefinition.getFieldType();
    if (fieldType == null) {
      throw new BException("FieldType in ColumnDefinition [{0}] cannot be null.", columnDefinition);
    }
    Map<String, String> props = columnDefinition.getProperties();
    try {
      return fieldManager.addColumnDefinition(family, columnName, subColumnName, fieldLessIndexed, fieldType, props);
    } catch (IOException e) {
      throw new BException(
          "Unknown error while trying to addColumnDefinition on table [{0}] with columnDefinition [{1}]", e, table,
          columnDefinition);
    }
  }

  private boolean inSafeMode(boolean useCache, String table) throws BlurException {
    String cluster = _clusterStatus.getCluster(useCache, table);
    if (cluster == null) {
      throw new BException("Table [" + table + "] not found.");
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

  public void setConfiguration(BlurConfiguration config) {
    _configuration = config;
  }

  @Override
  public Map<String, String> configuration() throws BlurException, TException {
    return _configuration.getProperties();
  }

  public int getMaxRecordsPerRowFetchRequest() {
    return _maxRecordsPerRowFetchRequest;
  }

  public void setMaxRecordsPerRowFetchRequest(int _maxRecordsPerRowFetchRequest) {
    this._maxRecordsPerRowFetchRequest = _maxRecordsPerRowFetchRequest;
  }
}
