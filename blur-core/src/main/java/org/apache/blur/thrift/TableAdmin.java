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
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.parsers.FactoryConfigurationError;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.analysis.FieldManager;
import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.command.Argument;
import org.apache.blur.command.BaseCommandManager;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.clusterstatus.ClusterStatus;
import org.apache.blur.server.TableContext;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.ArgumentDescriptor;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.CommandDescriptor;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.thrift.generated.Level;
import org.apache.blur.thrift.generated.Metric;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.TraceStorage;
import org.apache.blur.utils.MemoryReporter;
import org.apache.blur.utils.ShardUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.lucene.index.DirectoryReader;
import org.apache.zookeeper.ZooKeeper;

public abstract class TableAdmin implements Iface {

  private static final Log LOG = LogFactory.getLog(TableAdmin.class);
  protected ZooKeeper _zookeeper;
  protected ClusterStatus _clusterStatus;
  protected BlurConfiguration _configuration;
  protected int _maxRecordsPerRowFetchRequest = 1000;
  protected String _nodeName;

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
      ShardUtil.validateTableName(tableDescriptor.getName());
      assignClusterIfNull(tableDescriptor);
      List<String> tableList = _clusterStatus.getTableList(false, tableDescriptor.getCluster());
      if (!tableList.contains(tableDescriptor.getName())) {
        TableContext.clear(tableDescriptor.getName());
        _clusterStatus.createTable(tableDescriptor);
      } else {
        TableDescriptor existing = _clusterStatus.getTableDescriptor(false, tableDescriptor.getCluster(),
            tableDescriptor.getName());
        if (existing.equals(tableDescriptor)) {
          LOG.warn("Table [{0}] has already exists, but tried to create same table a second time.",
              tableDescriptor.getName());
        } else {
          LOG.warn("Table [{0}] has already exists.", tableDescriptor.getName());
          throw new BException("Table [{0}] has already exists.", tableDescriptor.getName());
        }
      }
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
      TableContext.clear(table);
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
      TableContext.clear(table);
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
      TableContext.clear(table);
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
      TableDescriptor tableDescriptor = _clusterStatus.getTableDescriptor(true, cluster, table);
      TableContext tableContext = TableContext.create(tableDescriptor);
      return tableContext.getDescriptor();
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
    boolean sortable = columnDefinition.isSortable();
    Map<String, String> props = columnDefinition.getProperties();
    boolean multiValueField = columnDefinition.isMultiValueField();
    try {
      return fieldManager.addColumnDefinition(family, columnName, subColumnName, fieldLessIndexed, fieldType, sortable,
          multiValueField, props);
    } catch (IOException e) {
      throw new BException(
          "Unknown error while trying to addColumnDefinition on table [{0}] with columnDefinition [{1}]", e, table,
          columnDefinition);
    }
  }

  @Override
  public List<String> traceList() throws BlurException, TException {
    TraceStorage storage = Trace.getStorage();
    try {
      return storage.getTraceIds();
    } catch (Exception e) {
      throw new BException("Unknown error while trying to get traceList", e);
    }
  }

  @Override
  public List<String> traceRequestList(String traceId) throws BlurException, TException {
    TraceStorage storage = Trace.getStorage();
    try {
      return storage.getRequestIds(traceId);
    } catch (Exception e) {
      throw new BException("Unknown error while trying to get traceRequestList for traceId [{0}]", e, traceId);
    }
  }

  @Override
  public String traceRequestFetch(String traceId, String requestId) throws BlurException, TException {
    TraceStorage storage = Trace.getStorage();
    try {
      return storage.getRequestContentsJson(traceId, requestId);
    } catch (Exception e) {
      throw new BException("Unknown error while trying to get traceRequestList for traceId [{0}] requestId [{1}]", e,
          traceId, requestId);
    }
  }

  @Override
  public void traceRemove(String traceId) throws BlurException, TException {
    TraceStorage storage = Trace.getStorage();
    try {
      storage.removeTrace(traceId);
    } catch (Exception e) {
      throw new BException("Unknown error while trying to get remove trace [{0}]", e, traceId);
    }
  }

  protected boolean inSafeMode(boolean useCache, String table) throws BlurException {
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

  @Override
  public Schema schema(String table) throws BlurException, TException {
    checkTable(table);
    try {
      TableContext tableContext = getTableContext(table);
      FieldManager fieldManager = tableContext.getFieldManager();
      fieldManager.loadFromStorage();
      Schema schema = new Schema().setTable(table);
      schema.setFamilies(new HashMap<String, Map<String, ColumnDefinition>>());
      Set<String> fieldNames = fieldManager.getFieldNames();
      INNER: for (String fieldName : fieldNames) {
        FieldTypeDefinition fieldTypeDefinition = fieldManager.getFieldTypeDefinition(fieldName);
        if (fieldTypeDefinition == null) {
          continue INNER;
        }
        String columnName = fieldTypeDefinition.getColumnName();
        String columnFamily = fieldTypeDefinition.getFamily();
        String subColumnName = fieldTypeDefinition.getSubColumnName();
        Map<String, ColumnDefinition> map = schema.getFamilies().get(columnFamily);
        if (map == null) {
          map = new HashMap<String, ColumnDefinition>();
          schema.putToFamilies(columnFamily, map);
        }
        if (subColumnName == null) {
          map.put(columnName, getColumnDefinition(fieldTypeDefinition));
        } else {
          map.put(columnName + "." + subColumnName, getColumnDefinition(fieldTypeDefinition));
        }
      }
      return schema;
    } catch (Exception e) {
      throw new BException("Unknown error while trying to get schema for table [{0}]", table);
    }
  }

  private static ColumnDefinition getColumnDefinition(FieldTypeDefinition fieldTypeDefinition) {
    ColumnDefinition columnDefinition = new ColumnDefinition();
    columnDefinition.setFamily(fieldTypeDefinition.getFamily());
    columnDefinition.setColumnName(fieldTypeDefinition.getColumnName());
    columnDefinition.setSubColumnName(fieldTypeDefinition.getSubColumnName());
    columnDefinition.setFieldLessIndexed(fieldTypeDefinition.isFieldLessIndexed());
    columnDefinition.setFieldType(fieldTypeDefinition.getFieldType());
    columnDefinition.setSortable(fieldTypeDefinition.isSortEnable());
    columnDefinition.setProperties(fieldTypeDefinition.getProperties());
    columnDefinition.setMultiValueField(fieldTypeDefinition.isMultiValueField());
    return columnDefinition;
  }

  private TableContext getTableContext(final String table) {
    return TableContext.create(_clusterStatus.getTableDescriptor(true, _clusterStatus.getCluster(true, table), table));
  }

  @Override
  public void ping() throws TException {

  }

  @Override
  public void logging(String classNameOrLoggerName, Level level) throws BlurException, TException {
    Logger logger;
    if (classNameOrLoggerName == null) {
      logger = LogManager.getRootLogger();
    } else {
      logger = LogManager.getLogger(classNameOrLoggerName);
    }

    if (logger == null) {
      throw new BException("Logger [{0}] not found.", classNameOrLoggerName);
    }
    org.apache.log4j.Level current = logger.getLevel();
    org.apache.log4j.Level newLevel = getLevel(level);
    LOG.info("Changing Logger [{0}] from logging level [{1}] to [{2}]", logger.getName(), current, newLevel);
    logger.setLevel(newLevel);
  }

  @Override
  public void resetLogging() throws BlurException, TException {
    try {
      reloadConfig();
    } catch (MalformedURLException e) {
      throw new BException("Unknown error while trying to reload log4j config.");
    } catch (FactoryConfigurationError e) {
      throw new BException("Unknown error while trying to reload log4j config.");
    }
  }

  protected List<CommandDescriptor> listInstalledCommands(BaseCommandManager commandManager) {
    List<CommandDescriptor> result = new ArrayList<CommandDescriptor>();
    Map<String, BigInteger> commands = new TreeMap<String, BigInteger>(commandManager.getCommands());
    for (Entry<String, BigInteger> e : commands.entrySet()) {
      CommandDescriptor commandDescriptor = new CommandDescriptor();
      String commandName = e.getKey();
      commandDescriptor.setCommandName(commandName);
      commandDescriptor.setVersion(e.getValue().toString(Character.MAX_RADIX));
      commandDescriptor.setDescription(commandManager.getDescription(commandName));
      commandDescriptor.setOptionalArguments(toThrift(commandManager.getOptionalArguments(commandName)));
      commandDescriptor.setRequiredArguments(toThrift(commandManager.getRequiredArguments(commandName)));
      commandDescriptor.setReturnType(commandManager.getReturnType(commandName));
      result.add(commandDescriptor);
    }
    return result;
  }

  private Map<String, ArgumentDescriptor> toThrift(Set<Argument> arguments) {
    Map<String, ArgumentDescriptor> result = new HashMap<String, ArgumentDescriptor>();
    for (Argument argument : arguments) {
      String name = argument.getName();
      result.put(name, new ArgumentDescriptor(name, argument.getType(), argument.getDescription()));
    }
    return result;
  }

  private void reloadConfig() throws MalformedURLException, FactoryConfigurationError, BException {
    String blurHome = System.getenv("BLUR_HOME");
    if (blurHome != null) {
      File blurHomeFile = new File(blurHome);
      if (blurHomeFile.exists()) {
        File log4jFile = new File(new File(blurHomeFile, "conf"), "log4j.xml");
        if (log4jFile.exists()) {
          LOG.info("Reseting log4j config from [{0}]", log4jFile);
          LogManager.resetConfiguration();
          DOMConfigurator.configure(log4jFile.toURI().toURL());
          return;
        }
      }
    }
    URL url = TableAdmin.class.getResource("/log4j.xml");
    if (url != null) {
      LOG.info("Reseting log4j config from classpath resource [{0}]", url);
      LogManager.resetConfiguration();
      DOMConfigurator.configure(url);
      return;
    }
    throw new BException("Could not locate log4j file to reload, doing nothing.");
  }

  private org.apache.log4j.Level getLevel(Level level) throws BlurException {
    switch (level) {
    case ALL:
      return org.apache.log4j.Level.ALL;
    case DEBUG:
      return org.apache.log4j.Level.DEBUG;
    case ERROR:
      return org.apache.log4j.Level.ERROR;
    case FATAL:
      return org.apache.log4j.Level.FATAL;
    case INFO:
      return org.apache.log4j.Level.INFO;
    case TRACE:
      return org.apache.log4j.Level.TRACE;
    case OFF:
      return org.apache.log4j.Level.OFF;
    case WARN:
      return org.apache.log4j.Level.WARN;
    default:
      throw new BException("Level [{0}] not found.", level);
    }
  }

  @Override
  public void bulkMutateStart(String bulkId) throws BlurException, TException {
    // TODO Start transaction here...
  }

  @Override
  public String configurationPerServer(String thriftServerPlusPort, String configName) throws BlurException, TException {
    if (thriftServerPlusPort == null || thriftServerPlusPort.equals(_nodeName)) {
      String s = _configuration.get(configName);
      if (s == null) {
        throw new BlurException("NOT_FOUND", null, ErrorType.UNKNOWN);
      }
      return s;
    }
    Iface client = BlurClient.getClient(thriftServerPlusPort);
    return client.configurationPerServer(thriftServerPlusPort, configName);
  }

  public String getNodeName() {
    return _nodeName;
  }

  public void setNodeName(String nodeName) {
    _nodeName = nodeName;
  }

  protected String getCluster(String table) throws BlurException, TException {
    TableDescriptor describe = describe(table);
    if (describe == null) {
      throw new BException("Table [" + table + "] not found.");
    }
    return describe.cluster;
  }

  @Override
  public void loadIndex(String table, List<String> externalIndexPaths) throws BlurException, TException {
    try {
      if (externalIndexPaths == null || externalIndexPaths.isEmpty()) {
        return;
      }
      String cluster = getCluster(table);
      TableDescriptor tableDescriptor = _clusterStatus.getTableDescriptor(true, cluster, table);
      TableContext tableContext = TableContext.create(tableDescriptor);
      Configuration configuration = tableContext.getConfiguration();
      Path tablePath = tableContext.getTablePath();
      FileSystem fileSystem = tablePath.getFileSystem(configuration);
      for (String externalPath : externalIndexPaths) {
        Path newLoadShardPath = new Path(externalPath);
        loadShard(newLoadShardPath, fileSystem, tablePath);
      }
    } catch (IOException e) {
      throw new BException(e.getMessage(), e);
    }
  }

  private void loadShard(Path newLoadShardPath, FileSystem fileSystem, Path tablePath) throws IOException {
    Path shardPath = new Path(tablePath, newLoadShardPath.getName());
    FileStatus[] listStatus = fileSystem.listStatus(newLoadShardPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".commit");
      }
    });

    for (FileStatus fileStatus : listStatus) {
      Path src = fileStatus.getPath();
      Path dst = new Path(shardPath, src.getName());
      if (fileSystem.rename(src, dst)) {
        LOG.info("Successfully moved [{0}] to [{1}].", src, dst);
      } else {
        LOG.info("Could not move [{0}] to [{1}].", src, dst);
        throw new IOException("Could not move [" + src + "] to [" + dst + "].");
      }
    }
  }

  @Override
  public void validateIndex(String table, List<String> externalIndexPaths) throws BlurException, TException {
    try {
      if (externalIndexPaths == null || externalIndexPaths.isEmpty()) {
        return;
      }
      String cluster = getCluster(table);
      TableDescriptor tableDescriptor = _clusterStatus.getTableDescriptor(true, cluster, table);
      TableContext tableContext = TableContext.create(tableDescriptor);
      Configuration configuration = tableContext.getConfiguration();
      Path tablePath = tableContext.getTablePath();
      FileSystem fileSystem = tablePath.getFileSystem(configuration);
      for (String externalPath : externalIndexPaths) {
        Path shardPath = new Path(externalPath);
        validateIndexesExist(shardPath, fileSystem, configuration);
      }
    } catch (IOException e) {
      throw new BException(e.getMessage(), e);
    }
  }

  private void validateIndexesExist(Path shardPath, FileSystem fileSystem, Configuration configuration)
      throws IOException {
    FileStatus[] listStatus = fileSystem.listStatus(shardPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".commit");
      }
    });
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      HdfsDirectory directory = new HdfsDirectory(configuration, path);
      try {
        if (!DirectoryReader.indexExists(directory)) {
          throw new IOException("Path [" + path + "] is not a valid index.");
        }
      } finally {
        directory.close();
      }
    }
  }

}
