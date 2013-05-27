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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.blur.MiniCluster;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.SimpleQuery;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.util.BlurThriftHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlurClusterTest {

  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "/tmp"));

  @BeforeClass
  public static void startCluster() throws IOException {
    LocalFileSystem localFS = FileSystem.getLocal(new Configuration());
    File testDirectory = new File(TMPDIR, "blur-cluster-test");
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

    MiniCluster.startBlurCluster("target/cluster", 2, 3);
  }

  @AfterClass
  public static void shutdownCluster() {
    MiniCluster.shutdownBlurCluster();
  }
  
  private Iface getClient() {
    return BlurClient.getClient(MiniCluster.getControllerConnectionStr());
  }

  @Test
  public void testCreateTable() throws BlurException, TException, IOException {
    Blur.Iface client = getClient();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test");
    tableDescriptor.setShardCount(5);
    tableDescriptor.setTableUri(MiniCluster.getFileSystemUri().toString() + "/blur/test");
    client.createTable(tableDescriptor);
    List<String> tableList = client.tableList();
    assertEquals(Arrays.asList("test"), tableList);
  }

  @Test
  public void testLoadTable() throws BlurException, TException, InterruptedException {
    Iface client = getClient();
    int length = 100;
    List<RowMutation> mutations = new ArrayList<RowMutation>();
    for (int i = 0; i < length; i++) {
      String rowId = UUID.randomUUID().toString();
      RecordMutation mutation = BlurThriftHelper.newRecordMutation("test", rowId, BlurThriftHelper.newColumn("test", "value"));
      RowMutation rowMutation = BlurThriftHelper.newRowMutation("test", rowId, mutation);
      rowMutation.setWaitToBeVisible(true);
      mutations.add(rowMutation);
    }
    long s = System.nanoTime();
    client.mutateBatch(mutations);
    long e = System.nanoTime();
    System.out.println("mutateBatch took [" + (e - s) / 1000000.0 + "]");
    BlurQuery blurQuery = new BlurQuery();
    SimpleQuery simpleQuery = new SimpleQuery();
    simpleQuery.setQueryStr("test.test:value");
    blurQuery.setSimpleQuery(simpleQuery);
    BlurResults results = client.query("test", blurQuery);
    assertEquals(length, results.getTotalResults());
  }
  
  @Test
  public void testTestShardFailover() throws BlurException, TException, InterruptedException, IOException, KeeperException {
    Iface client = getClient();
    int length = 100;
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.setUseCacheIfPresent(false);
    SimpleQuery simpleQuery = new SimpleQuery();
    simpleQuery.setQueryStr("test.test:value");
    blurQuery.setSimpleQuery(simpleQuery);
    BlurResults results1 = client.query("test", blurQuery);
    assertEquals(length, results1.getTotalResults());
    
    MiniCluster.killShardServer(1);
    
    //make sure the WAL syncs
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    
    //This should block until shards have failed over
    client.shardServerLayout("test");
    
    assertEquals(length, client.query("test", blurQuery).getTotalResults());
    
  }

  @Test
  public void testCreateDisableAndRemoveTable() throws IOException, BlurException, TException {
    Iface client = getClient();
    String tableName = UUID.randomUUID().toString();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(tableName);
    tableDescriptor.setShardCount(5);
    tableDescriptor.setTableUri(MiniCluster.getFileSystemUri().toString() + "/blur/" + tableName);

    for (int i = 0; i < 3; i++) {
      client.createTable(tableDescriptor);
      client.disableTable(tableName);
      client.removeTable(tableName, true);
    }

    assertFalse(client.tableList().contains(tableName));

  }
}
