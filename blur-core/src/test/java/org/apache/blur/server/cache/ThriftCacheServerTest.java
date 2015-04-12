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
package org.apache.blur.server.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;
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
import org.junit.Before;
import org.junit.Test;

public class ThriftCacheServerTest {

  private BlurConfiguration _configuration;
  private ThriftCache _thriftCache;
  private ThriftCacheServer _thriftCacheServer;
  private String _table;

  @Before
  public void setup() throws IOException {
    _configuration = new BlurConfiguration();
    _thriftCache = new ThriftCache(10000);
    _thriftCacheServer = new ThriftCacheServer(_configuration, getMock(), getMockIndexServer(), _thriftCache);
    _table = "t";
  }

  @Test
  public void testQuery() throws IOException, BlurException, TException {
    _thriftCache.clear();

    assertEquals(0, _thriftCache.getMisses());
    assertEquals(0, _thriftCache.getHits());

    BlurQuery blurQuery1 = new BlurQuery();
    blurQuery1.setUserContext("user1");
    BlurResults blurResults1 = _thriftCacheServer.query(_table, blurQuery1);

    assertEquals(1, _thriftCache.getMisses());
    assertEquals(0, _thriftCache.getHits());

    BlurQuery blurQuery2 = new BlurQuery();
    blurQuery1.setUserContext("user2");
    BlurResults blurResults2 = _thriftCacheServer.query(_table, blurQuery2);

    assertEquals(1, _thriftCache.getMisses());
    assertEquals(1, _thriftCache.getHits());

    assertFalse(blurResults1 == blurResults2);
    assertEquals(blurResults1, blurResults2);
  }

  @Test
  public void testTableStats() throws IOException, BlurException, TException {
    _thriftCache.clear();

    assertEquals(0, _thriftCache.getMisses());
    assertEquals(0, _thriftCache.getHits());

    TableStats tableStats1 = _thriftCacheServer.tableStats(_table);

    assertEquals(1, _thriftCache.getMisses());
    assertEquals(0, _thriftCache.getHits());

    TableStats tableStats2 = _thriftCacheServer.tableStats(_table);

    assertEquals(1, _thriftCache.getMisses());
    assertEquals(1, _thriftCache.getHits());

    assertFalse(tableStats1 == tableStats2);
    assertEquals(tableStats1, tableStats2);
  }

  @Test
  public void testFetchRow() throws BlurException, TException {
    _thriftCache.clear();

    assertEquals(0, _thriftCache.getMisses());
    assertEquals(0, _thriftCache.getHits());

    Selector selector1 = new Selector();
    selector1.setLocationId("1");
    FetchResult fetchRow1 = _thriftCacheServer.fetchRow(_table, selector1);

    assertEquals(1, _thriftCache.getMisses());
    assertEquals(0, _thriftCache.getHits());

    Selector selector2 = new Selector();
    selector2.setLocationId("1");
    FetchResult fetchRow2 = _thriftCacheServer.fetchRow(_table, selector2);

    assertEquals(1, _thriftCache.getMisses());
    assertEquals(1, _thriftCache.getHits());

    assertFalse(fetchRow1 == fetchRow2);
    assertEquals(fetchRow1, fetchRow2);
  }

  @Test
  public void testFetchRowBatch1() throws BlurException, TException {
    _thriftCache.clear();

    assertEquals(0, _thriftCache.getMisses());
    assertEquals(0, _thriftCache.getHits());

    Selector selector1 = new Selector();
    selector1.setLocationId("1");
    List<FetchResult> fetchRowBatch1 = _thriftCacheServer.fetchRowBatch(_table, Arrays.asList(selector1));

    assertEquals(1, _thriftCache.getMisses());
    assertEquals(0, _thriftCache.getHits());

    Selector selector2 = new Selector();
    selector2.setLocationId("1");
    List<FetchResult> fetchRowBatch2 = _thriftCacheServer.fetchRowBatch(_table, Arrays.asList(selector2));

    assertEquals(1, _thriftCache.getMisses());
    assertEquals(1, _thriftCache.getHits());

    assertFalse(fetchRowBatch1 == fetchRowBatch2);
    assertEquals(fetchRowBatch1, fetchRowBatch2);
  }

  @Test
  public void testFetchRowBatch2() throws BlurException, TException {
    _thriftCache.clear();

    assertEquals(0, _thriftCache.getMisses());
    assertEquals(0, _thriftCache.getHits());

    Selector selector1 = new Selector();
    selector1.setLocationId("1");
    List<FetchResult> fetchRowBatch1 = _thriftCacheServer.fetchRowBatch(_table, Arrays.asList(selector1));

    assertEquals(1, _thriftCache.getMisses());
    assertEquals(0, _thriftCache.getHits());

    Selector selector2 = new Selector();
    selector2.setLocationId("1");
    List<FetchResult> fetchRowBatch2 = _thriftCacheServer.fetchRowBatch(_table, Arrays.asList(selector2));

    assertEquals(1, _thriftCache.getMisses());
    assertEquals(1, _thriftCache.getHits());

    assertFalse(fetchRowBatch1 == fetchRowBatch2);
    assertEquals(fetchRowBatch1, fetchRowBatch2);

    Selector selector3 = new Selector();
    selector3.setLocationId("2");
    List<FetchResult> fetchRowBatch3 = _thriftCacheServer.fetchRowBatch(_table, Arrays.asList(selector1, selector3));

    // one miss for the non cached selector
    assertEquals(2, _thriftCache.getMisses());
    // one hit for the cached selector
    assertEquals(2, _thriftCache.getHits());

    Selector selector4 = new Selector();
    selector4.setLocationId("2");
    List<FetchResult> fetchRowBatch4 = _thriftCacheServer.fetchRowBatch(_table, Arrays.asList(selector2, selector4));

    assertEquals(2, _thriftCache.getMisses());
    // two hits for the cached selectors
    assertEquals(4, _thriftCache.getHits());

    assertFalse(fetchRowBatch3 == fetchRowBatch4);
    assertEquals(fetchRowBatch3, fetchRowBatch4);

  }

  private Iface getMock() {
    return new Iface() {

      @Override
      public List<String> traceRequestList(String traceId) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public String traceRequestFetch(String traceId, String requestId) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void traceRemove(String traceId) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<String> traceList() throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<String> terms(String table, String columnFamily, String columnName, String startWith, short size)
          throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public TableStats tableStats(String table) throws BlurException, TException {
        return new TableStats();
      }

      @Override
      public List<String> tableListByCluster(String cluster) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<String> tableList() throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void startTrace(String traceId, String requestId) throws TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<String> shardServerList(String cluster) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public Map<String, Map<String, ShardState>> shardServerLayoutState(String table) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<String> shardClusterList() throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void setUser(User user) throws TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public Schema schema(String table) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void resetLogging() throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void removeTable(String table, boolean deleteIndexFiles) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void removeSnapshot(String table, String name) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void refresh() throws TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long recordFrequency(String table, String columnFamily, String columnName, String value)
          throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public Response reconnect(long instanceExecutionId) throws BlurException, TimeoutException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<String> queryStatusIdList(String table) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public BlurQueryStatus queryStatusById(String table, String uuid) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public BlurResults query(String table, BlurQuery blurQuery) throws BlurException, TException {
        BlurResults blurResults = new BlurResults();
        return blurResults;
      }

      @Override
      public void ping() throws TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public String parseQuery(String table, Query query) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void optimize(String table, int numberOfSegmentsPerShard) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void mutateBatch(List<RowMutation> mutations) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void mutate(RowMutation mutation) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public Map<String, Metric> metrics(Set<String> metrics) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void logging(String classNameOrLoggerName, Level level) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void loadData(String table, String location) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public Map<String, List<String>> listSnapshots(String table) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<CommandDescriptor> listInstalledCommands() throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public boolean isInSafeMode(String cluster) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<FetchResult> fetchRowBatch(String table, List<Selector> selectors) throws BlurException, TException {
        List<FetchResult> list = new ArrayList<FetchResult>();
        for (Selector selector : selectors) {
          list.add(fetchRow(table, selector));
        }
        return list;
      }

      @Override
      public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
        FetchResult fetchResult = new FetchResult();
        return fetchResult;
      }

      @Override
      public Response execute(String commandName, Arguments arguments) throws BlurException, TimeoutException,
          TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void enqueueMutateBatch(List<RowMutation> mutations) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void enqueueMutate(RowMutation mutation) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void enableTable(String table) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void disableTable(String table) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public TableDescriptor describe(String table) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void createTable(TableDescriptor tableDescriptor) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void createSnapshot(String table, String name) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<String> controllerServerList() throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public Map<String, String> configuration() throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<String> commandStatusList(int startingAt, short fetch, CommandStatusState state)
          throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public CommandStatus commandStatus(String commandExecutionId) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void commandCancel(String commandExecutionId) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void cancelQuery(String table, String uuid) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void bulkMutateStart(String bulkId) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void bulkMutateFinish(String bulkId, boolean apply, boolean blockUntilComplete) throws BlurException,
          TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void bulkMutateAddMultiple(String bulkId, List<RowMutation> rowMutations) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void bulkMutateAdd(String bulkId, RowMutation rowMutation) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public boolean addColumnDefinition(String table, ColumnDefinition columnDefinition) throws BlurException,
          TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public String configurationPerServer(String thriftServerPlusPort, String configName) throws BlurException,
          TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void validateIndex(String table, List<String> externalIndexPaths) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void loadIndex(String table, List<String> externalIndexPaths) throws BlurException, TException {
        throw new RuntimeException("Not implemented.");
      }

    };
  }

  private IndexServer getMockIndexServer() {
    return new IndexServer() {

      @Override
      public long getTableSize(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public Map<String, ShardState> getShardState(String table) {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public SortedSet<String> getShardListCurrentServerOnly(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getRowCount(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getRecordCount(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public String getNodeName() {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public Map<String, BlurIndex> getIndexes(String table) throws IOException {
        Map<String, BlurIndex> map = new HashMap<String, BlurIndex>();
        map.put("shard-000000", null);
        map.put("shard-000001", null);
        return map;
      }

      @Override
      public void close() throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getSegmentImportInProgressCount(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getSegmentImportPendingCount(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }
    };
  }
}
