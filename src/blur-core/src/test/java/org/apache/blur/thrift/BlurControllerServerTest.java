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

import org.junit.Test;

public class BlurControllerServerTest {

  @Test
  public void testNothing() {
    // do nothing
  }

  // private static final String TABLE = "test";
  // private static Map<String, Iface> shardServers = new HashMap<String,
  // Iface>();
  // private static BlurControllerServer server;
  // private static Thread daemonService;
  //
  // @BeforeClass
  // public static void zkServerStartup() throws InterruptedException,
  // KeeperException, IOException, BlurException, TException {
  // rm(new File("../zk-tmp"));
  // daemonService = new Thread(new Runnable() {
  // @Override
  // public void run() {
  // QuorumPeerMain.main(new
  // String[]{"./src/test/resources/com/nearinfinity/blur/thrift/zoo.cfg"});
  // }
  // });
  // daemonService.start();
  // Thread.sleep(1000);
  //
  // ZooKeeper zookeeper = new ZooKeeper("127.0.0.1:10101", 30000, new Watcher()
  // {
  // @Override
  // public void process(WatchedEvent event) {
  //
  // }
  // });
  //
  // addShardServer("shard-00000000");
  // addShardServer("shard-00000001");
  // addShardServer("shard-00000002");
  // BlurQueryChecker queryChecker = new BlurQueryChecker(new
  // BlurConfiguration());
  // server = new BlurControllerServer();
  // server.setClient(getClient());
  // server.setClusterStatus(new ZookeeperClusterStatus(zookeeper));
  // server.setQueryChecker(queryChecker);
  // server.setZookeeper(zookeeper);
  // server.init();
  //
  // File file = new File("./tmp-data/test");
  // rm(file);
  //
  // TableDescriptor tableDescriptor = new TableDescriptor();
  // tableDescriptor.name = "test";
  // tableDescriptor.shardCount = 3;
  // tableDescriptor.tableUri = file.toURI().toString();
  // tableDescriptor.analyzerDefinition = new AnalyzerDefinition();
  // server.createTable(tableDescriptor);
  // }
  //
  // @AfterClass
  // public static void zkServerShutdown() {
  // daemonService.interrupt();
  // server.close();
  // }
  //
  // private static void rm(File file) {
  // if (file.isDirectory()) {
  // for (File f : file.listFiles()) {
  // rm(f);
  // }
  // }
  // file.delete();
  // }
  //
  // @Test
  // public void testQuery() throws BlurException, TException {
  // BlurQuery blurQuery = new BlurQuery();
  // blurQuery.maxQueryTime = TimeUnit.SECONDS.toMillis(5);
  // blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
  // BlurResults results = server.query(TABLE, blurQuery);
  // assertNotNull(results);
  // }
  //
  // @Test
  // public void testQueryWithFacets() throws BlurException, TException {
  // BlurQuery blurQuery = new BlurQuery();
  // blurQuery.maxQueryTime = TimeUnit.SECONDS.toMillis(5);
  // blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
  // blurQuery.facets = new ArrayList<Facet>();
  // blurQuery.facets.add(new Facet());
  // blurQuery.facets.add(new Facet());
  // BlurResults results = server.query(TABLE, blurQuery);
  // assertNotNull(results);
  // assertNotNull(results.facetCounts);
  // for (int i = 0; i < results.facetCounts.size(); i++) {
  // long count = results.facetCounts.get(i);
  // assertEquals(shardServers.size() * (i + 1), count);
  // }
  // }
  //
  // @Test
  // public void testRecordFrequency() throws BlurException, TException {
  // long recordFrequency = server.recordFrequency(TABLE, "cf", "cn", "value");
  // assertEquals(shardServers.size(), recordFrequency);
  // }
  //
  // @Test
  // public void testMutate() throws BlurException, TException {
  // RowMutation mutation = new RowMutation();
  // mutation.setRowId("1234");
  // RecordMutation recMut = new RecordMutation();
  // Record record = new Record();
  // record.setFamily("test");
  // record.setRecordId("5678");
  // record.addToColumns(new Column("name", "value"));
  // mutation.addToRecordMutations(recMut);
  // mutation.table = TABLE;
  // server.mutate(mutation);
  //
  // Selector selector = new Selector();
  // selector.rowId = "1234";
  //
  // FetchResult fetchRow = server.fetchRow(TABLE, selector);
  // assertNotNull(fetchRow.rowResult);
  // }
  //
  // private static BlurClient getClient() {
  // BlurClientEmbedded blurClientEmbedded = new BlurClientEmbedded();
  // for (String node : shardServers.keySet()) {
  // blurClientEmbedded.putNode(node, shardServers.get(node));
  // }
  // return blurClientEmbedded;
  // }
  //
  // private static Iface getShardServer(final String node) {
  // return new Iface() {
  //
  // private Map<String, Map<String, Row>> rows = new HashMap<String,
  // Map<String, Row>>();
  //
  // @Override
  // public List<String> terms(String arg0, String arg1, String arg2, String
  // arg3, short arg4) throws BlurException, TException {
  // throw new RuntimeException("no impl");
  // }
  //
  // @Override
  // public List<String> tableList() throws BlurException, TException {
  // List<String> table = new ArrayList<String>();
  // table.add(TABLE);
  // return table;
  // }
  //
  // @Override
  // public List<String> shardServerList(String cluster) throws BlurException,
  // TException {
  // throw new RuntimeException("no impl");
  // }
  //
  // @Override
  // public Map<String, String> shardServerLayout(String table) throws
  // BlurException, TException {
  // Map<String, String> layout = new HashMap<String, String>();
  // layout.put(node, node);
  // return layout;
  // }
  //
  // @Override
  // public BlurResults query(String table, BlurQuery query) throws
  // BlurException, TException {
  // BlurResults results = new BlurResults();
  // results.putToShardInfo(node, 0);
  // results.setFacetCounts(getFacetCounts(query));
  // return results;
  // }
  //
  // private List<Long> getFacetCounts(BlurQuery query) {
  // if (query.facets != null) {
  // int size = query.facets.size();
  // List<Long> results = new ArrayList<Long>();
  // for (int i = 0; i < size; i++) {
  // results.add(i + 1L);
  // }
  // return results;
  // }
  // return null;
  // }
  //
  // @Override
  // public Schema schema(String arg0) throws BlurException, TException {
  // throw new RuntimeException("no impl");
  // }
  //
  // @Override
  // public long recordFrequency(String arg0, String arg1, String arg2, String
  // arg3) throws BlurException, TException {
  // return 1l;
  // }
  //
  // @Override
  // public FetchResult fetchRow(String table, Selector selector) throws
  // BlurException, TException {
  // Map<String, Row> map = rows.get(table);
  // Row row = map.get(selector.rowId);
  // FetchResult fetchResult = new FetchResult();
  // fetchResult.setRowResult(new FetchRowResult(row));
  // return fetchResult;
  // }
  //
  // @Override
  // public TableDescriptor describe(String arg0) throws BlurException,
  // TException {
  // TableDescriptor descriptor = new TableDescriptor();
  // descriptor.isEnabled = true;
  // descriptor.shardCount = 3;
  // return descriptor;
  // }
  //
  // @Override
  // public List<BlurQueryStatus> currentQueries(String arg0) throws
  // BlurException, TException {
  // throw new RuntimeException("no impl");
  // }
  //
  // @Override
  // public List<String> controllerServerList() throws BlurException, TException
  // {
  // throw new RuntimeException("no impl");
  // }
  //
  // @Override
  // public void cancelQuery(String table, long arg0) throws BlurException,
  // TException {
  // throw new RuntimeException("no impl");
  // }
  //
  // private Row toRow(RowMutation mutation) {
  // Row row = new Row();
  // row.id = mutation.rowId;
  // row.records = toRecords(mutation.recordMutations);
  // return row;
  // }
  //
  // private List<Record> toRecords(List<RecordMutation> recordMutations) {
  // List<Record> records = new ArrayList<Record>();
  // for (RecordMutation mutation : recordMutations) {
  // records.add(mutation.record);
  // }
  // return records;
  // }
  //
  // @Override
  // public void createTable(TableDescriptor tableDescriptor) throws
  // BlurException, TException {
  // throw new RuntimeException("not impl");
  // }
  //
  // @Override
  // public void disableTable(String table) throws BlurException, TException {
  // throw new RuntimeException("not impl");
  // }
  //
  // @Override
  // public void enableTable(String table) throws BlurException, TException {
  // throw new RuntimeException("not impl");
  // }
  //
  // @Override
  // public void removeTable(String table, boolean deleteIndexFiles) throws
  // BlurException, TException {
  // throw new RuntimeException("not impl");
  // }
  //
  // @Override
  // public TableStats getTableStats(String table) throws BlurException,
  // TException {
  // return new TableStats();
  // }
  //
  // @Override
  // public void mutate(RowMutation mutation) throws BlurException, TException {
  // String table = mutation.table;
  // Map<String, Row> map = rows.get(table);
  // if (map == null) {
  // map = new HashMap<String, Row>();
  // rows.put(table, map);
  // }
  // Row row = toRow(mutation);
  // map.put(row.id, row);
  // }
  //
  // @Override
  // public void mutateBatch(List<RowMutation> mutations) throws BlurException,
  // TException {
  // for (RowMutation mutation : mutations) {
  // MutationHelper.validateMutation(mutation);
  // }
  // for (RowMutation mutation : mutations) {
  // mutate(mutation);
  // }
  // }
  //
  // @Override
  // public List<String> shardClusterList() throws BlurException, TException {
  // throw new RuntimeException("no impl");
  // }
  // };
  // }
  //
  // private static void addShardServer(String node) {
  // shardServers.put(node, getShardServer(node));
  // }

}
