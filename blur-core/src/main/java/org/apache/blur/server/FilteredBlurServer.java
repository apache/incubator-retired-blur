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
package org.apache.blur.server;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Arguments;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.CommandDescriptor;
import org.apache.blur.thrift.generated.CommandStatus;
import org.apache.blur.thrift.generated.CommandStatusState;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Level;
import org.apache.blur.thrift.generated.Metric;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Response;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.thrift.generated.TimeoutException;
import org.apache.blur.thrift.generated.User;

public class FilteredBlurServer implements Iface {

  protected final Iface _iface;

  public void startTrace(String rootId, String requestId) throws TException {
    _iface.startTrace(rootId, requestId);
  }

  protected final boolean _shard;
  protected final BlurConfiguration _configuration;

  public FilteredBlurServer(BlurConfiguration configuration, Iface iface, boolean shard) {
    _iface = iface;
    _shard = shard;
    _configuration = configuration;
  }

  public void setUser(User user) throws TException {
    _iface.setUser(user);
  }

  public final BlurServerContext getServerContext() {
    if (_shard) {
      return ShardServerContext.getShardServerContext();
    } else {
      return ControllerServerContext.getControllerServerContext();
    }
  }

  public void createTable(TableDescriptor tableDescriptor) throws BlurException, TException {
    _iface.createTable(tableDescriptor);
  }

  public void enableTable(String table) throws BlurException, TException {
    _iface.enableTable(table);
  }

  public void disableTable(String table) throws BlurException, TException {
    _iface.disableTable(table);
  }

  public void removeTable(String table, boolean deleteIndexFiles) throws BlurException, TException {
    _iface.removeTable(table, deleteIndexFiles);
  }

  public boolean addColumnDefinition(String table, ColumnDefinition columnDefinition) throws BlurException, TException {
    return _iface.addColumnDefinition(table, columnDefinition);
  }

  public List<String> tableList() throws BlurException, TException {
    return _iface.tableList();
  }

  public List<String> tableListByCluster(String cluster) throws BlurException, TException {
    return _iface.tableListByCluster(cluster);
  }

  public TableDescriptor describe(String table) throws BlurException, TException {
    return _iface.describe(table);
  }

  public Schema schema(String table) throws BlurException, TException {
    return _iface.schema(table);
  }

  public String parseQuery(String table, Query query) throws BlurException, TException {
    return _iface.parseQuery(table, query);
  }

  public TableStats tableStats(String table) throws BlurException, TException {
    return _iface.tableStats(table);
  }

  public void optimize(String table, int numberOfSegmentsPerShard) throws BlurException, TException {
    _iface.optimize(table, numberOfSegmentsPerShard);
  }

  public void createSnapshot(String table, String name) throws BlurException, TException {
    _iface.createSnapshot(table, name);
  }

  public void removeSnapshot(String table, String name) throws BlurException, TException {
    _iface.removeSnapshot(table, name);
  }

  public Map<String, List<String>> listSnapshots(String table) throws BlurException, TException {
    return _iface.listSnapshots(table);
  }

  public BlurResults query(String table, BlurQuery blurQuery) throws BlurException, TException {
    return _iface.query(table, blurQuery);
  }

  public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
    return _iface.fetchRow(table, selector);
  }

  public List<FetchResult> fetchRowBatch(String table, List<Selector> selectors) throws BlurException, TException {
    return _iface.fetchRowBatch(table, selectors);
  }

  public void mutate(RowMutation mutation) throws BlurException, TException {
    _iface.mutate(mutation);
  }

  public void mutateBatch(List<RowMutation> mutations) throws BlurException, TException {
    _iface.mutateBatch(mutations);
  }

  public void cancelQuery(String table, String uuid) throws BlurException, TException {
    _iface.cancelQuery(table, uuid);
  }

  public List<String> queryStatusIdList(String table) throws BlurException, TException {
    return _iface.queryStatusIdList(table);
  }

  public BlurQueryStatus queryStatusById(String table, String uuid) throws BlurException, TException {
    return _iface.queryStatusById(table, uuid);
  }

  public List<String> terms(String table, String columnFamily, String columnName, String startWith, short size)
      throws BlurException, TException {
    return _iface.terms(table, columnFamily, columnName, startWith, size);
  }

  public long recordFrequency(String table, String columnFamily, String columnName, String value) throws BlurException,
      TException {
    return _iface.recordFrequency(table, columnFamily, columnName, value);
  }

  public List<String> shardClusterList() throws BlurException, TException {
    return _iface.shardClusterList();
  }

  public List<String> shardServerList(String cluster) throws BlurException, TException {
    return _iface.shardServerList(cluster);
  }

  public List<String> controllerServerList() throws BlurException, TException {
    return _iface.controllerServerList();
  }

  public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
    return _iface.shardServerLayout(table);
  }

  public Map<String, Map<String, ShardState>> shardServerLayoutState(String table) throws BlurException, TException {
    return _iface.shardServerLayoutState(table);
  }

  public boolean isInSafeMode(String cluster) throws BlurException, TException {
    return _iface.isInSafeMode(cluster);
  }

  public Map<String, String> configuration() throws BlurException, TException {
    return _iface.configuration();
  }

  public Map<String, Metric> metrics(Set<String> metrics) throws BlurException, TException {
    return _iface.metrics(metrics);
  }

  public List<String> traceList() throws BlurException, TException {
    return _iface.traceList();
  }

  public List<String> traceRequestList(String traceId) throws BlurException, TException {
    return _iface.traceRequestList(traceId);
  }

  public String traceRequestFetch(String traceId, String requestId) throws BlurException, TException {
    return _iface.traceRequestFetch(traceId, requestId);
  }

  public void traceRemove(String traceId) throws BlurException, TException {
    _iface.traceRemove(traceId);
  }

  @Override
  public void ping() throws TException {
    _iface.ping();
  }

  @Override
  public void logging(String classNameOrLoggerName, Level level) throws BlurException, TException {
    _iface.logging(classNameOrLoggerName, level);
  }

  @Override
  public void resetLogging() throws BlurException, TException {
    _iface.resetLogging();
  }

  @Override
  public void enqueueMutate(RowMutation mutation) throws BlurException, TException {
    _iface.enqueueMutate(mutation);
  }

  @Override
  public void enqueueMutateBatch(List<RowMutation> mutations) throws BlurException, TException {
    _iface.enqueueMutateBatch(mutations);
  }

  @Override
  public Response execute(String commandName, Arguments arguments) throws BlurException, TException {
    return _iface.execute(commandName, arguments);
  }

  @Override
  public void refresh() throws TException {
    _iface.refresh();
  }

  @Override
  public List<String> commandStatusList(int startingAt, short fetch, CommandStatusState state) throws BlurException,
      TException {
    return _iface.commandStatusList(startingAt, fetch, state);
  }

  @Override
  public List<CommandDescriptor> listInstalledCommands() throws BlurException, TException {
    return _iface.listInstalledCommands();
  }

  @Override
  public Response reconnect(long instanceExecutionId) throws BlurException, TimeoutException, TException {
    return _iface.reconnect(instanceExecutionId);
  }

  @Override
  public CommandStatus commandStatus(String commandExecutionId) throws BlurException, TException {
    return _iface.commandStatus(commandExecutionId);
  }

  @Override
  public void commandCancel(String commandExecutionId) throws BlurException, TException {
    _iface.commandCancel(commandExecutionId);
  }

  @Override
  public void loadData(String table, String location) throws BlurException, TException {
    _iface.loadData(table, location);
  }

  @Override
  public void bulkMutateStart(String bulkId) throws BlurException, TException {
    _iface.bulkMutateStart(bulkId);
  }

  @Override
  public void bulkMutateAdd(String bulkId, RowMutation rowMutation) throws BlurException, TException {
    _iface.bulkMutateAdd(bulkId, rowMutation);
  }

  @Override
  public void bulkMutateFinish(String bulkId, boolean apply, boolean blockUntilComplete) throws BlurException,
      TException {
    _iface.bulkMutateFinish(bulkId, apply, blockUntilComplete);
  }

  @Override
  public void bulkMutateAddMultiple(String bulkId, List<RowMutation> rowMutations) throws BlurException, TException {
    _iface.bulkMutateAddMultiple(bulkId, rowMutations);
  }

  @Override
  public String configurationPerServer(String thriftServerPlusPort, String configName) throws BlurException, TException {
    return _iface.configurationPerServer(thriftServerPlusPort, configName);
  }

}
