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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.BlurShardName;
import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.indexserver.DistributedManager;
import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants;
import com.nearinfinity.blur.zookeeper.ZkUtils;

public class CreateTable {
    
    private static Log LOG = LogFactory.getLog(CreateTable.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        String zkConnectionStr = args[0];
        String table = args[1];
        BlurAnalyzer analyzer = BlurAnalyzer.create(new File(args[2]));
        String uri = args[3];
        String shardCount = args[4];
        CompressionCodec codec = getInstance(args[5]);
        String blockSize = args[6];

        ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr);
        ZookeeperDistributedManager dm = new ZookeeperDistributedManager();
        dm.setZooKeeper(zooKeeper);
        createTable(dm,table,analyzer,uri,Integer.parseInt(shardCount),codec,Integer.parseInt(blockSize));
    }
    
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

    public static void createTable(DistributedManager dm, String table, BlurAnalyzer analyzer, String uri, int shardCount, CompressionCodec compressionCodec, int compressionBlockSize) throws IOException {
        if (dm.exists(ZookeeperPathConstants.getBlurTablesPath(), table)) {
            throw new IOException("Table [" + table + "] already exists.");
        }
        setupFileSystem(uri,shardCount);
        dm.createPath(ZookeeperPathConstants.getBlurTablesPath(), table);
        dm.createPath(ZookeeperPathConstants.getBlurTablesPath(), table, ZookeeperPathConstants.getBlurTablesUri());
        dm.createPath(ZookeeperPathConstants.getBlurTablesPath(), table, ZookeeperPathConstants.getBlurTablesShardCount());
        dm.createPath(ZookeeperPathConstants.getBlurTablesPath(), table, ZookeeperPathConstants.getBlurTablesCompressionCodec());
        dm.createPath(ZookeeperPathConstants.getBlurTablesPath(), table, ZookeeperPathConstants.getBlurTablesCompressionBlockSize());
        dm.saveData(analyzer.toJSON().getBytes(),             ZookeeperPathConstants.getBlurTablesPath(), table);
        dm.saveData(uri.getBytes(),                           ZookeeperPathConstants.getBlurTablesPath(), table, ZookeeperPathConstants.getBlurTablesUri());
        dm.saveData(Integer.toString(shardCount).getBytes() , ZookeeperPathConstants.getBlurTablesPath(), table, ZookeeperPathConstants.getBlurTablesShardCount());
        dm.saveData(compressionCodec.getClass().getName().getBytes() , ZookeeperPathConstants.getBlurTablesPath(), table, ZookeeperPathConstants.getBlurTablesCompressionCodec());
        dm.saveData(Integer.toString(compressionBlockSize).getBytes() , ZookeeperPathConstants.getBlurTablesPath(), table, ZookeeperPathConstants.getBlurTablesCompressionBlockSize());
        dm.createPath(ZookeeperPathConstants.getBlurLockPath(table));
    }

    private static void setupFileSystem(String uri, int shardCount) throws IOException {
        Path tablePath = new Path(uri);
        FileSystem fileSystem = FileSystem.get(tablePath.toUri(), new Configuration());
        if (createPath(fileSystem,tablePath)) {
            LOG.info("Table uri existed.");
            validateShardCount(shardCount,fileSystem,tablePath);
        }
        for (int i = 0; i < shardCount; i++) {
            String shardName = BlurShardName.getShardName(SHARD_PREFIX, i);
            Path shardPath = new Path(tablePath, shardName);
            createPath(fileSystem, shardPath);
        }
    }

    private static void validateShardCount(int shardCount, FileSystem fileSystem, Path tablePath) throws IOException {
        FileStatus[] listStatus = fileSystem.listStatus(tablePath);
        if (listStatus.length != shardCount) {
            LOG.error("Number of directories in table path [" + tablePath + 
            		"] does not match definition of [" + shardCount + 
            		"] shard count.");
            throw new RuntimeException("Number of directories in table path [" + tablePath + 
                    "] does not match definition of [" + shardCount + 
                    "] shard count.");
        }
    }

    private static boolean createPath(FileSystem fileSystem, Path path) throws IOException {
        if (!fileSystem.exists(path)) {
            LOG.info("Path [{0}] does not exist, creating.",path);
            fileSystem.mkdirs(path);
            return false;
        }
        return true;
    }

}
