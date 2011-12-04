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
import org.apache.lucene.search.Similarity;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.manager.clusterstatus.ZookeeperPathConstants;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurUtil;

public class CreateTable {

  private static Log LOG = LogFactory.getLog(CreateTable.class);

  @SuppressWarnings("unchecked")
  public static <T> T getInstance(String className, Class<T> c) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class<?> clazz = Class.forName(className);
    return (T) configure(clazz.newInstance());
  }

  private static <T> T configure(T t) {
    if (t instanceof Configurable) {
      Configurable configurable = (Configurable) t;
      configurable.setConf(new Configuration());
    }
    return t;
  }

  public static void createTable(ZooKeeper zookeeper, TableDescriptor tableDescriptor) throws IOException, KeeperException, InterruptedException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    String table = tableDescriptor.name;
    String cluster = tableDescriptor.cluster;
    BlurAnalyzer analyzer = new BlurAnalyzer(tableDescriptor.analyzerDefinition);
    String uri = tableDescriptor.tableUri;
    int shardCount = tableDescriptor.shardCount;
    CompressionCodec compressionCodec = getInstance(tableDescriptor.compressionClass, CompressionCodec.class);
    int compressionBlockSize = tableDescriptor.compressionBlockSize;
    Similarity similarity = getInstance(tableDescriptor.similarityClass, Similarity.class);
    if (tableDescriptor.similarityClass == null) {
      similarity = new FairSimilarity();
    } else {
      similarity = getInstance(tableDescriptor.similarityClass, Similarity.class);
    }
    boolean blockCaching = tableDescriptor.blockCaching;
    Set<String> blockCachingFileTypes = tableDescriptor.blockCachingFileTypes;

    String blurTablePath = ZookeeperPathConstants.getTablePath(cluster, table);

    if (zookeeper.exists(blurTablePath, false) != null) {
      throw new IOException("Table [" + table + "] already exists.");
    }
    setupFileSystem(uri, shardCount);
    createPath(zookeeper, blurTablePath, analyzer.toJSON().getBytes());
    createPath(zookeeper, ZookeeperPathConstants.getTableUriPath(cluster, table), uri.getBytes());
    createPath(zookeeper, ZookeeperPathConstants.getTableShardCountPath(cluster, table), Integer.toString(shardCount).getBytes());
    createPath(zookeeper, ZookeeperPathConstants.getTableCompressionCodecPath(cluster, table), compressionCodec.getClass().getName().getBytes());
    createPath(zookeeper, ZookeeperPathConstants.getTableCompressionBlockSizePath(cluster, table), Integer.toString(compressionBlockSize).getBytes());
    createPath(zookeeper, ZookeeperPathConstants.getTableSimilarityPath(cluster, table), similarity.getClass().getName().getBytes());
    createPath(zookeeper, ZookeeperPathConstants.getLockPath(cluster, table), null);
    if (blockCaching) {
      createPath(zookeeper, ZookeeperPathConstants.getTableBlockCachingPath(cluster, table), null);
    }
    createPath(zookeeper, ZookeeperPathConstants.getTableBlockCachingFileTypesPath(cluster, table), toBytes(blockCachingFileTypes));
  }

  private static byte[] toBytes(Set<String> blockCachingFileTypes) {
    if (blockCachingFileTypes == null || blockCachingFileTypes.isEmpty()) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    for (String type : blockCachingFileTypes) {
      builder.append(type).append(',');
    }
    return builder.substring(0, builder.length() - 1).getBytes();
  }

  private static void createPath(ZooKeeper zookeeper, String path, byte[] data) throws KeeperException, InterruptedException {
    zookeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
