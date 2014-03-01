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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
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
import org.apache.blur.utils.GCWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
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
  public void addedDocumentShouldShowUpInBlur() throws SolrServerException, IOException, BlurException, TException {
    String table = "addedDocumentShouldShowUpInBlur";
    createTable(table);
    SolrServer server = new SolrLookingBlurServer(miniCluster.getControllerConnectionStr(), table);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    doc.addField("fam.value", "123");

    server.add(doc);

    TableStats stats = client().tableStats(table);

    assertEquals("We should have one record.", 1, stats.recordCount);
    assertEquals("We should have one row.", 1, stats.rowCount);

    assertTotalResults(table, "fam.value:123", 1l);

    removeTable(table);
  }

  @Test
  public void childDocsShouldBecomeRecordsOfRow() throws SolrServerException, IOException, BlurException, TException {
    String table = "childDocsShouldBecomeRecordsOfRow";
    createTable(table);
    SolrServer server = new SolrLookingBlurServer(miniCluster.getControllerConnectionStr(), table);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    
    List<SolrInputDocument> children = Lists.newArrayList();
    for(int i = 0; i < 100; i++) {
      SolrInputDocument child = new SolrInputDocument();
      child.addField("id", i);
      child.addField("fam.key", "value" + i);
      children.add(child);
    }
    doc.addChildDocuments(children);
    
    server.add(doc);

    TableStats stats = client().tableStats(table);

    assertEquals("We should have one record.", 100, stats.recordCount);
    assertEquals("We should have one row.", 1, stats.rowCount);

    assertTotalResults(table, "fam.key:value1", 1l);
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
    doc.addField("id", "1");
    doc.addField("fam.value", "123");
    doc.addField("fam.value", "124");

    server.add(doc);

    assertTotalResults(table, "fam.value:123", 1l);
    assertTotalResults(table, "fam.value:124", 1l);
    assertTotalResults(table, "fam.value:justincase", 0l);

    removeTable(table);
  }

  @Test
  public void documentsShouldBeAbleToBeIndexedInBatch() throws SolrServerException, IOException, BlurException,
      TException {
    String table = "multipleDocumentsShouldBeIndexed";
    createTable(table);

    SolrServer server = new SolrLookingBlurServer(miniCluster.getControllerConnectionStr(), table);

    List<SolrInputDocument> docs = Lists.newArrayList();

    for (int i = 0; i < 100; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("fam.value", "12" + i);
      docs.add(doc);
    }

    server.add(docs);

    assertTotalResults(table, "fam.value:123", 1l);
    assertTotalResults(table, "fam.value:124", 1l);
    assertTotalResults(table, "rowid:1", 1l);
    assertTotalResults(table, "rowid:2", 1l);
    assertTotalResults(table, "fam.value:1299", 1l);
    assertTotalResults(table, "fam.value:justincase", 0l);

    removeTable(table);
  }

  @Test
  public void weShouldBeAbleToDeleteARowById() throws SolrServerException, IOException, BlurException,
      TException {
    String table = "weShouldBeAbleToDeleteARowById";
    createTable(table);
    SolrServer server = new SolrLookingBlurServer(miniCluster.getControllerConnectionStr(), table);
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField("id", "1");
    doc1.addField("fam.value", "123");
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField("id", "2");
    doc2.addField("fam.value", "124");
    List<SolrInputDocument> docs = Lists.newArrayList(doc1, doc2);

    server.add(docs);

    assertTotalResults(table, "rowid:1", 1l);
    assertTotalResults(table, "rowid:2", 1l);

    server.deleteById("1");

    assertTotalResults(table, "rowid:1", 0l);
    assertTotalResults(table, "rowid:2", 1l);

    removeTable(table);
  }

  @Test
  public void weShouldBeAbleToDeleteARowByAListOfIds() throws SolrServerException, IOException, BlurException,
      TException {
    String table = "weShouldBeAbleToDeleteARowByAListOfIds";
    createTable(table);
    SolrServer server = new SolrLookingBlurServer(miniCluster.getControllerConnectionStr(), table);
    for (int i = 0; i < 20; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("fam.value", "value" + i);
      server.add(doc);
    }

    assertTotalResults(table, "rowid:1", 1l);
    assertTotalResults(table, "rowid:2", 1l);
    List<String> ids = Lists.newArrayList("1", "2", "3", "4", "5");
    server.deleteById(ids);

    for (String id : ids) {
      assertTotalResults(table, "rowid:" + id, 0l);
    }

    removeTable(table);
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

}
