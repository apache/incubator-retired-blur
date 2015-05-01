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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.blur.MiniCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlurBlockPlacementPolicyDefaultTest {

  private static final String DFS_BLOCKREPORT_INTERVAL_MSEC = "dfs.blockreport.intervalMsec";
  private static final String DFS_BLOCK_REPLICATOR_CLASSNAME = "dfs.block.replicator.classname";
  private static MiniCluster _miniCluster;

  @BeforeClass
  public static void setupClass() {
    _miniCluster = new MiniCluster();
    Configuration conf = new Configuration();
    conf.set(DFS_BLOCK_REPLICATOR_CLASSNAME, BlurBlockPlacementPolicyDefault.class.getName());
    conf.set(BlurBlockPlacementPolicyDefault.BLUR_BLOCK_PLACEMENT_SERVER_LOOKUP, TestServerLookup.class.getName());
    conf.setLong(DFS_BLOCKREPORT_INTERVAL_MSEC, 10 * 1000);
    boolean format = true;
    String path = "./target/tmp/BlurBlockPlacementPolicyDefaultTest";
    String[] racks = new String[] { "/r1", "/r1", "/r2", "/r2", "/r3", "/r3" };
    _miniCluster.startDfs(conf, format, path, racks);
  }

  @AfterClass
  public static void teardownClass() {
    _miniCluster.shutdownDfs();
  }

  @Test
  public void test1() throws IOException, InterruptedException {
    FileSystem fileSystem = _miniCluster.getFileSystem();
    String rootStr = fileSystem.getUri().toString();
    Path root = new Path(rootStr + "/");
    fileSystem.mkdirs(new Path(root, "/test/table/shard-00000000"));
    Path p = writeFile(fileSystem, "/test/table/shard-00000000/test1");
    String shardServer = "host4.foo.com";
    assertBlocksExistOnShardServer(fileSystem, p, shardServer);
    setReplication(fileSystem, p, 4);
    assertBlocksExistOnShardServer(fileSystem, p, shardServer);
    setReplication(fileSystem, p, 5);
    assertBlocksExistOnShardServer(fileSystem, p, shardServer);
    setReplication(fileSystem, p, 1);
    assertBlocksExistOnShardServer(fileSystem, p, shardServer);
  }

  @Test
  public void test2() throws IOException, InterruptedException {
    FileSystem fileSystem = _miniCluster.getFileSystem();
    String rootStr = fileSystem.getUri().toString();
    Path root = new Path(rootStr + "/");
    fileSystem.mkdirs(new Path(root, "/test/table/shard-00000000"));

    String shardServer = "host4.foo.com";
    Path p = writeFileNotOnShardServer(fileSystem, "/testfile", shardServer);
    Path dst = new Path(root, "/test/table/shard-00000000/test2");
    fileSystem.rename(p, dst);
    p = dst;

    setReplication(fileSystem, p, 2);

    assertBlocksExistOnShardServer(fileSystem, p, shardServer);
    setReplication(fileSystem, p, 4);
    assertBlocksExistOnShardServer(fileSystem, p, shardServer);
    setReplication(fileSystem, p, 5);
    assertBlocksExistOnShardServer(fileSystem, p, shardServer);
    setReplication(fileSystem, p, 1);
    assertBlocksExistOnShardServer(fileSystem, p, shardServer);
  }

  private Path writeFileNotOnShardServer(FileSystem fileSystem, String path, String shardServer) throws IOException {
    String rootStr = fileSystem.getUri().toString();
    Path p = new Path(rootStr + path);
    boolean fail = true;
    OUTER: while (fail) {
      fail = false;
      FSDataOutputStream outputStream = fileSystem.create(p, (short) 1);
      byte[] buf = new byte[1000];
      for (int i = 0; i < 1000; i++) {
        outputStream.write(buf);
      }
      outputStream.close();
      FileStatus fileStatus = fileSystem.getFileStatus(p);
      BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(p, 0, fileStatus.getLen());
      for (BlockLocation blockLocation : blockLocations) {
        fail = Arrays.asList(blockLocation.getHosts()).contains(shardServer);
        if (fail) {
          continue OUTER;
        }
      }
    }
    return p;
  }

  private void setReplication(FileSystem fileSystem, Path p, int rep) throws IOException, InterruptedException {
    fileSystem.setReplication(p, (short) rep);
    waitForReplication(fileSystem, p, rep);
  }

  private void waitForReplication(FileSystem fileSystem, Path p, int replicas) throws IOException, InterruptedException {
    FileStatus fileStatus = fileSystem.getFileStatus(p);
    boolean fail = true;
    while (fail) {
      fail = false;
      BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(p, 0, fileStatus.getLen());
      for (BlockLocation blockLocation : blockLocations) {
        System.out.println(blockLocation);
        String[] hosts = blockLocation.getHosts();
        if (hosts.length != replicas) {
          fail = true;
        }
      }
      Thread.sleep(1000);
    }
  }

  private void assertBlocksExistOnShardServer(FileSystem fileSystem, Path p, String shardServer) throws IOException {
    FileStatus fileStatus = fileSystem.getFileStatus(p);
    BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(p, 0, fileStatus.getLen());
    for (BlockLocation blockLocation : blockLocations) {
      System.out.println(blockLocation);
      String[] hosts = blockLocation.getHosts();
      assertTrue(Arrays.asList(hosts).contains(shardServer));
    }
  }

  private Path writeFile(FileSystem fileSystem, String path) throws IOException {
    String rootStr = fileSystem.getUri().toString();
    Path p = new Path(rootStr + path);
    FSDataOutputStream outputStream = fileSystem.create(p);
    byte[] buf = new byte[1000];
    for (int i = 0; i < 1000; i++) {
      outputStream.write(buf);
    }
    outputStream.close();
    return p;
  }

}
