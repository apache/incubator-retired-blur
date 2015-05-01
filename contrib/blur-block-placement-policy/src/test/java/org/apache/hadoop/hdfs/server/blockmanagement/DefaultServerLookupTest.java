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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.blur.MiniCluster;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DefaultServerLookupTest {

  private static MiniCluster _miniCluster;

  @BeforeClass
  public static void setupClass() {
    _miniCluster = new MiniCluster();
    _miniCluster.startBlurCluster("./target/tmp/DefaultServerLookupTest", 1, 2, true);
  }

  @AfterClass
  public static void teardownClass() {
    _miniCluster.shutdownBlurCluster();
  }

  @Test
  public void test() throws BlurException, TException, IOException, InterruptedException {
    String zkConnectionString = _miniCluster.getZkConnectionString();
    Configuration configuration = new Configuration(_miniCluster.getConfiguration());
    configuration.set(DefaultServerLookup.BLUR_ZK_CONNECTIONS, zkConnectionString);
    DefaultServerLookup defaultServerLookup = new DefaultServerLookup(configuration, null);
    Thread.sleep(1000);
    assertFalse(defaultServerLookup.isPathSupported("/test"));

    String tableUri = _miniCluster.getFileSystemUri().toString() + "/blur/tables/test";

    Iface client = BlurClient.getClientFromZooKeeperConnectionStr(zkConnectionString);

    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test");
    tableDescriptor.setShardCount(3);
    tableDescriptor.setTableUri(tableUri);
    client.createTable(tableDescriptor);

    FileSystem fileSystem = _miniCluster.getFileSystem();
    Path shardPath = new Path(tableUri, "shard-00000000");
    FileStatus[] listStatus = fileSystem.listStatus(shardPath);
    for (FileStatus fileStatus : listStatus) {
      System.out.println(fileStatus.getPath());
    }
    Path filePath = new Path(shardPath, "write.lock");

    assertTrue(defaultServerLookup.isPathSupported(filePath.toUri().getPath()));

    client.disableTable(tableDescriptor.getName());

    Thread.sleep(15000);

    assertFalse(defaultServerLookup.isPathSupported(filePath.toUri().getPath()));

  }
}
