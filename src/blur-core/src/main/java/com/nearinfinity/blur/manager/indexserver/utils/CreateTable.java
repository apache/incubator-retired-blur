/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.manager.indexserver.utils;

import static com.nearinfinity.blur.utils.BlurConstants.SHARD_PREFIX;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurUtil;

public class CreateTable {

  private static Log LOG = LogFactory.getLog(CreateTable.class);
  
//  public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, NumberFormatException, KeeperException,
//      InterruptedException {
//    String zkConnectionStr = args[0];
//    String table = args[1];
//    BlurAnalyzer analyzer = BlurAnalyzer.create(new File(args[2]));
//    String uri = args[3];
//    String shardCount = args[4];
//    CompressionCodec codec = getInstance(args[5]);
//    String blockSize = args[6];
//
//    ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr);
//    createTable(zooKeeper, table, analyzer, uri, Integer.parseInt(shardCount), codec, Integer.parseInt(blockSize));
//  }

  public static CompressionCodec getInstance(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class<?> clazz = Class.forName(className);
    return configure((CompressionCodec) clazz.newInstance());
  }

  private static CompressionCodec configure(CompressionCodec codec) {
    if (codec instanceof Configurable) {
      Configurable configurable = (Configurable) codec;
      configurable.setConf(new Configuration());
    }
    return codec;
  }

  public static void createTable(ZooKeeper zookeeper, TableDescriptor tableDescriptor) throws IOException, KeeperException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    String blurTablesPath = ZookeeperPathConstants.getBlurTablesPath();
    
    String table = tableDescriptor.name;
    BlurAnalyzer analyzer = new BlurAnalyzer(tableDescriptor.analyzerDefinition);
    String uri = tableDescriptor.tableUri;
    int shardCount = tableDescriptor.shardCount;
    CompressionCodec compressionCodec = getInstance(tableDescriptor.compressionClass);
    int compressionBlockSize = tableDescriptor.compressionBlockSize;
    
    boolean blockCaching = tableDescriptor.blockCaching;
    Set<String> blockCachingFileTypes = tableDescriptor.blockCachingFileTypes;
    
    if (zookeeper.exists(blurTablesPath + "/" + table, false) != null) {
      throw new IOException("Table [" + table + "] already exists.");
    }
    setupFileSystem(uri, shardCount);
    createPath(zookeeper, analyzer.toJSON().getBytes(), blurTablesPath, table);
    createPath(zookeeper, uri.getBytes(), blurTablesPath, table, ZookeeperPathConstants.getBlurTablesUri());
    createPath(zookeeper, Integer.toString(shardCount).getBytes(), blurTablesPath, table, ZookeeperPathConstants.getBlurTablesShardCount());
    createPath(zookeeper, compressionCodec.getClass().getName().getBytes(), blurTablesPath, table, ZookeeperPathConstants.getBlurTablesCompressionCodec());
    createPath(zookeeper, Integer.toString(compressionBlockSize).getBytes(), blurTablesPath, table, ZookeeperPathConstants.getBlurTablesCompressionBlockSize());
    createPath(zookeeper, null, ZookeeperPathConstants.getBlurLockPath(table));
    if (blockCaching) {
      createPath(zookeeper, null, blurTablesPath, table, ZookeeperPathConstants.getBlurTablesBlockCaching());
    }
    createPath(zookeeper, toBytes(blockCachingFileTypes), blurTablesPath, table, ZookeeperPathConstants.getBlurTablesBlockCachingFileTypes());
  }

  private static byte[] toBytes(Set<String> blockCachingFileTypes) {
    if (blockCachingFileTypes == null || blockCachingFileTypes.isEmpty()) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    for (String type : blockCachingFileTypes) {
      builder.append(type).append(',');
    }
    return builder.substring(0, builder.length()-1).getBytes();
  }

  private static void createPath(ZooKeeper zookeeper, byte[] data, String... pathes) throws KeeperException, InterruptedException {
    String path = getPath(pathes);
    zookeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  private static String getPath(String... pathes) {
    StringBuilder builder = new StringBuilder();
    for (String p : pathes) {
      builder.append('/').append(removeLeadingAndTrailingSlashes(p));
    }
    return builder.toString();
  }

  private static Object removeLeadingAndTrailingSlashes(String p) {
    if (p.startsWith("/")) {
      p = p.substring(1);
    }
    if (p.endsWith("/")) {
      return p.substring(0, p.length() - 1);
    }
    return p;
  }

  private static void setupFileSystem(String uri, int shardCount) throws IOException {
    Path tablePath = new Path(uri);
    FileSystem fileSystem = FileSystem.get(tablePath.toUri(), new Configuration());
    if (createPath(fileSystem, tablePath)) {
      LOG.info("Table uri existed.");
      validateShardCount(shardCount, fileSystem, tablePath);
    }
    for (int i = 0; i < shardCount; i++) {
      String shardName = BlurUtil.getShardName(SHARD_PREFIX, i);
      Path shardPath = new Path(tablePath, shardName);
      createPath(fileSystem, shardPath);
    }
  }

  private static void validateShardCount(int shardCount, FileSystem fileSystem, Path tablePath) throws IOException {
    FileStatus[] listStatus = fileSystem.listStatus(tablePath);
    if (listStatus.length != shardCount) {
      LOG.error("Number of directories in table path [" + tablePath + "] does not match definition of [" + shardCount + "] shard count.");
      throw new RuntimeException("Number of directories in table path [" + tablePath + "] does not match definition of [" + shardCount + "] shard count.");
    }
  }

  private static boolean createPath(FileSystem fileSystem, Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      LOG.info("Path [{0}] does not exist, creating.", path);
      fileSystem.mkdirs(path);
      return false;
    }
    return true;
  }
}
