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
package org.apache.blur.slur;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.blur.MiniCluster;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.GCWatcher;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SolrLookingBlurServerTest {
  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_BlurClusterTest"));
  private static MiniCluster miniCluster;
  private static String connectionStr;

  @BeforeClass
  public static void startCluster() throws IOException {
    GCWatcher.init(0.60);
    LocalFileSystem localFS = FileSystem.getLocal(new Configuration());
    File testDirectory = new File(TMPDIR, "blur-cluster-test").getAbsoluteFile();
    testDirectory.mkdirs();

    Path directory = new Path(testDirectory.getPath());
    FsPermission dirPermissions = localFS.getFileStatus(directory).getPermission();
    FsAction userAction = dirPermissions.getUserAction();
    FsAction groupAction = dirPermissions.getGroupAction();
    FsAction otherAction = dirPermissions.getOtherAction();

    StringBuilder builder = new StringBuilder();
    builder.append(userAction.ordinal());
    builder.append(groupAction.ordinal());
    builder.append(otherAction.ordinal());
    String dirPermissionNum = builder.toString();
    System.setProperty("dfs.datanode.data.dir.perm", dirPermissionNum);
    testDirectory.delete();
    miniCluster = new MiniCluster();
    miniCluster.startBlurCluster(new File(testDirectory, "cluster").getAbsolutePath(), 2, 3, true);
    connectionStr = miniCluster.getControllerConnectionStr();

  }

  @AfterClass
  public static void shutdownCluster() {
    miniCluster.shutdownBlurCluster();
  }

  @Test
  public void childDocsShouldBecomeRecordsOfRow() throws Exception {
    String table = "childDocsShouldBecomeRecordsOfRow";

    TestTableCreator.newTable(table)
        .withRowCount(1).withRecordsPerRow(100)
        .withRecordColumns("fam.value").create();

    TableStats stats = client().tableStats(table);

    assertEquals("We should have one record.", 100, stats.recordCount);
    assertEquals("We should have one row.", 1, stats.rowCount);

    assertTotalResults(table, "fam.value:value0-0", 1l);
    assertTotalRecordResults(table, "recordid:99", 1l);

    removeTable(table);
  }

  @Test
  public void docShouldBeDiscoverableWithMultiValuedFields() throws SolrServerException, IOException, BlurException,
      TException {
    String table = "docShouldBeDiscoverableWithMultiValuedFields";
    createTable(table);
    SolrServer server = new SolrLookingBlurServer(miniCluster.getControllerConnectionStr(), table);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("rowid", "1");

    SolrInputDocument child = new SolrInputDocument();
    child.addField("recordid", "1");
    child.addField("fam.value", "123");
    child.addField("fam.value", "124");

    doc.addChildDocument(child);

    server.add(doc);

    assertTotalResults(table, "fam.value:123", 1l);
    assertTotalResults(table, "fam.value:124", 1l);
    assertTotalResults(table, "fam.value:justincase", 0l);

    removeTable(table);
  }

  @Test
  public void documentsShouldBeAbleToBeIndexedInBatch() throws Exception,
      TException {
    String table = "multipleDocumentsShouldBeIndexed";
    TestTableCreator.newTable(table)
        .withRowCount(100).withRecordsPerRow(1)
        .withRecordColumns("fam.value").create();

    assertTotalResults(table, "fam.value:value0-0", 1l);
    assertTotalResults(table, "fam.value:value1-0", 1l);
    assertTotalResults(table, "rowid:1", 1l);
    assertTotalResults(table, "rowid:2", 1l);
    assertTotalResults(table, "fam.value:value99-0", 1l);
    assertTotalResults(table, "fam.value:justincase", 0l);

    removeTable(table);
  }

  @Test
  public void weShouldBeAbleToDeleteARowById() throws Exception,
      TException {
    String table = "weShouldBeAbleToDeleteARowById";

    SolrServer server = TestTableCreator.newTable(table).withRowCount(2).withRecordsPerRow(1)
        .withRecordColumns("fam.value").create();

    assertTotalResults(table, "rowid:0", 1l);
    assertTotalResults(table, "rowid:1", 1l);

    server.deleteById("1");

    assertTotalResults(table, "rowid:0", 1l);
    assertTotalResults(table, "rowid:1", 0l);

    removeTable(table);
  }

  @Test
  public void weShouldBeAbleToDeleteARowByAListOfIds() throws Exception,
      TException {
    String table = "weShouldBeAbleToDeleteARowByAListOfIds";

    SolrServer server = TestTableCreator.newTable(table).withRowCount(20).withRecordsPerRow(1)
        .withRecordColumns("fam.value").create();

    assertTotalResults(table, "rowid:1", 1l);
    assertTotalResults(table, "rowid:2", 1l);
    List<String> ids = Lists.newArrayList("1", "2", "3", "4", "5");
    server.deleteById(ids);

    for (String id : ids) {
      assertTotalResults(table, "rowid:" + id, 0l);
    }

    removeTable(table);
  }

  @Test
  public void basicFullTextQuery() throws Exception {
    String table = "basicFullTextQuery";
    SolrServer server = TestTableCreator.newTable(table)
        .withRowCount(1).withRecordsPerRow(2)
        .withRecordColumns("fam.value", "fam.mvf", "fam.mvf").create();

    SolrQuery query = new SolrQuery("value0-0");

    QueryResponse response = server.query(query);

    assertEquals("We should get our doc back.", 1l, response.getResults().getNumFound());

    SolrDocument docResult = response.getResults().get(0);

    assertEquals("0", docResult.getFieldValue("recordid"));
    assertEquals("value0-0", docResult.getFieldValue("fam.value"));

    Collection<Object> mvfVals = docResult.getFieldValues("fam.mvf");

    assertTrue("We should get all our values back[" + mvfVals + "]",
        CollectionUtils.isEqualCollection(mvfVals, Lists.newArrayList("value0-0", "value0-0")));

    removeTable(table);
  }

  @Test
  public void fieldsRequestsShouldTurnIntoSelectors() throws Exception,
      TException {

    String table = "fieldsRequestsShouldTurnIntoSelectors";
    SolrServer server = TestTableCreator.newTable(table)
        .withRowCount(1).withRecordsPerRow(2)
        .withRecordColumns("fam.value", "fam.mvf").create();

    SolrQuery query = new SolrQuery("value0-0");
    query.setFields("fam.value");
    QueryResponse response = server.query(query);

    assertEquals("We should get our doc back for a valid test.", 1l, response.getResults().getNumFound());

    SolrDocument docResult = response.getResults().get(0);

    assertEquals("value0-0", docResult.getFieldValue("fam.value"));
    assertNull("We shouldn't get this one back since it wasnt in our fields.", docResult.getFieldValues("fam.mvf"));

    removeTable(table);
  }

  @Test
  public void weShouldBeAbleToPageResults() throws SolrServerException, IOException, BlurException, TException {
    String table = "weShouldBeAbleToPageResults";
    SolrServer server = createServerAndTableWithSimpleTestDoc(table);

    SolrQuery query = new SolrQuery("123");
    query.setFields("fam.value");
    QueryResponse response = server.query(query);

    assertEquals("We should get our doc back for a valid test.", 1l, response.getResults().getNumFound());

    SolrDocument docResult = response.getResults().get(0);

    assertEquals("123", docResult.getFieldValue("fam.value"));
    assertNull("We shouldn't get this one back since it wasnt in our fields.", docResult.getFieldValues("fam.mvf"));

    removeTable(table);
  }

  private SolrServer createServerAndTableWithSimpleTestDoc(String table) throws BlurException, TException, IOException,
      SolrServerException {
    createTable(table);
    SolrServer server = new SolrLookingBlurServer(miniCluster.getControllerConnectionStr(), table);
    SolrInputDocument doc;

    doc = createSimpleTestDoc();

    server.add(doc);
    return server;
  }

  private SolrInputDocument createSimpleTestDoc() {
    SolrInputDocument doc;
    doc = new SolrInputDocument();
    doc.addField("rowid", "1");
    SolrInputDocument child = new SolrInputDocument();
    child.addField("recordid", "1");
    child.addField("fam.value", "123");
    child.addField("fam.mvf", "aaa");
    child.addField("fam.mvf", "bbb");
    doc.addChildDocument(child);
    return doc;
  }

  private void assertTotalResults(String table, String q, long expected) throws BlurException, TException {
    BlurQuery bquery = new BlurQuery();
    Query query = new Query();
    query.setQuery(q);
    bquery.setQuery(query);
    BlurResults results = client().query(table, bquery);

    assertEquals("Should find our row.", expected, results.getTotalResults());
  }

  private void assertTotalRecordResults(String table, String q, long expected) throws BlurException, TException {
    BlurQuery bquery = new BlurQuery();
    Query query = new Query();
    query.setQuery(q);
    query.setRowQuery(false);
    bquery.setQuery(query);

    BlurResults results = client().query(table, bquery);

    assertEquals("Should find our record.", expected, results.getTotalResults());

  }

  private void removeTable(String table) {
    try {
      client().disableTable(table);
    } catch (Exception e) {

    }
    try {
      client().removeTable(table, true);
    } catch (Exception e) {

    }
  }

  private static void createTable(String tableName) throws BlurException, TException, IOException {
    Blur.Iface client = client();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(tableName);
    tableDescriptor.setShardCount(5);
    tableDescriptor.setTableUri(miniCluster.getFileSystemUri().toString() + "/blur/" + tableName);
    client.createTable(tableDescriptor);
    List<String> tableList = client.tableList();
    assertTrue(tableList.contains(tableName));
  }

  private static Iface client() {
    return BlurClient.getClient(connectionStr);
  }

  private static class TestTableCreator {
    private int rows = 1;
    private int recordsPerRow = 10;
    private String tableName;
    private String[] columns = { "fam.value" };

    private TestTableCreator(String table) {
      this.tableName = table;
    }

    public static TestTableCreator newTable(String table) {
      return new TestTableCreator(table);
    }

    public TestTableCreator withRowCount(int rows) {
      this.rows = rows;
      return this;
    }

    public TestTableCreator withRecordsPerRow(int count) {
      this.recordsPerRow = count;
      return this;
    }

    public TestTableCreator withRecordColumns(String... cols) {
      this.columns = cols;
      return this;
    }

    public SolrServer create() throws Exception {
      createTable(tableName);
      SolrServer server = new SolrLookingBlurServer(miniCluster.getControllerConnectionStr(), tableName);

      for (int i = 0; i < rows; i++) {
        SolrInputDocument parent = new SolrInputDocument();
        parent.addField(BlurConstants.ROW_ID, i);
        for (int j = 0; j < recordsPerRow; j++) {
          SolrInputDocument child = new SolrInputDocument();
          child.addField(BlurConstants.RECORD_ID, j);

          for (String colName : columns) {
            child.addField(colName, "value" + i + "-" + j);
          }
          parent.addChildDocument(child);
        }
        server.add(parent);
      }
      return server;
    }
  }

}
