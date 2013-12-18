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
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Metric;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
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

}
