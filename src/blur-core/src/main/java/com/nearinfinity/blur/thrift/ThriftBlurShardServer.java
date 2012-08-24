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

package com.nearinfinity.blur.thrift;

<<<<<<< HEAD
import static com.nearinfinity.blur.utils.BlurConstants.*;
=======
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_CONTROLLER_BIND_PORT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_GUI_CONTROLLER_PORT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_GUI_SHARD_PORT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT;
>>>>>>> gui
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_MAX_CLAUSE_COUNT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BIND_ADDRESS;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BIND_PORT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BLOCKCACHE_DIRECT_MEMORY_ALLOCATION;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BLOCKCACHE_SLAB_COUNT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_CACHE_MAX_QUERYCACHE_ELEMENTS;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_CACHE_MAX_TIMETOLIVE;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_DATA_FETCH_THREAD_COUNT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_FILTER_CACHE_CLASS;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_HOSTNAME;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_INDEX_DELETION_POLICY_MAXAGE;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_INDEX_WARMUP_CLASS;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_OPENER_THREAD_COUNT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_SAFEMODEDELAY;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_TIME_BETWEEN_COMMITS;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_TIME_BETWEEN_REFRESHS;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_ZOOKEEPER_SYSTEM_TIME_TOLERANCE;
import static com.nearinfinity.blur.utils.BlurUtil.quietClose;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.concurrent.SimpleUncaughtExceptionHandler;
import com.nearinfinity.blur.concurrent.ThreadWatcher;
import com.nearinfinity.blur.gui.HttpJettyServer;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.lucene.index.TimeBasedIndexDeletionPolicy;
import com.nearinfinity.blur.manager.BlurFilterCache;
import com.nearinfinity.blur.manager.BlurQueryChecker;
import com.nearinfinity.blur.manager.DefaultBlurFilterCache;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.clusterstatus.ZookeeperClusterStatus;
import com.nearinfinity.blur.manager.indexserver.BlurIndexWarmup;
import com.nearinfinity.blur.manager.indexserver.BlurServerShutDown;
import com.nearinfinity.blur.manager.indexserver.BlurServerShutDown.BlurShutdown;
import com.nearinfinity.blur.manager.indexserver.DefaultBlurIndexWarmup;
import com.nearinfinity.blur.manager.indexserver.DistributedIndexServer;
import com.nearinfinity.blur.manager.writer.BlurIndexRefresher;
import com.nearinfinity.blur.metrics.BlurMetrics;
import com.nearinfinity.blur.store.BufferStore;
import com.nearinfinity.blur.store.blockcache.BlockCache;
import com.nearinfinity.blur.store.blockcache.BlockDirectory;
import com.nearinfinity.blur.store.blockcache.BlockDirectoryCache;
import com.nearinfinity.blur.store.blockcache.Cache;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurUtil;
import com.nearinfinity.blur.zookeeper.ZkUtils;

public class ThriftBlurShardServer extends ThriftServer {

  private static final Log LOG = LogFactory.getLog(ThriftBlurShardServer.class);

  public static void main(String[] args) throws Exception {
    int serverIndex = getServerIndex(args);
    LOG.info("Setting up Shard Server");

    Thread.setDefaultUncaughtExceptionHandler(new SimpleUncaughtExceptionHandler());
    BlurConfiguration configuration = new BlurConfiguration();

    ThriftServer server = createServer(serverIndex, configuration);
    server.start();
  }

  public static ThriftServer createServer(int serverIndex, BlurConfiguration configuration) throws Exception {
    // setup block cache
    // 134,217,728 is the bank size, therefore there are 16,384 block
    // in a bank when using a block of 8,192
    int numberOfBlocksPerBank = 16384;
    int blockSize = BlockDirectory.BLOCK_SIZE;
    int bankCount = configuration.getInt(BLUR_SHARD_BLOCKCACHE_SLAB_COUNT, 1);
    Cache cache;
    Configuration config = new Configuration();
    BlurMetrics blurMetrics = new BlurMetrics(config);
    if (bankCount >= 1) {
      BlockCache blockCache;
      boolean directAllocation = configuration.getBoolean(BLUR_SHARD_BLOCKCACHE_DIRECT_MEMORY_ALLOCATION, true);

      int slabSize = numberOfBlocksPerBank * blockSize;
      LOG.info("Number of slabs of block cache [{0}] with direct memory allocation set to [{1}]", bankCount, directAllocation);
      LOG.info("Block cache target memory usage, slab size of [{0}] will allocate [{1}] slabs and use ~[{2}] bytes", slabSize, bankCount, ((long) bankCount * (long) slabSize));

      BufferStore.init(configuration, blurMetrics);

      try {
        long totalMemory = (long) bankCount * (long) numberOfBlocksPerBank * (long) blockSize;
        blockCache = new BlockCache(blurMetrics, directAllocation, totalMemory, slabSize, blockSize);
      } catch (OutOfMemoryError e) {
        if ("Direct buffer memory".equals(e.getMessage())) {
          System.err
              .println("The max direct memory is too low.  Either increase by setting (-XX:MaxDirectMemorySize=<size>g -XX:+UseLargePages) or disable direct allocation by (blur.shard.blockcache.direct.memory.allocation=false) in blur-site.properties");
          System.exit(1);
        }
        throw e;
      }
      cache = new BlockDirectoryCache(blockCache, blurMetrics);
    } else {
      cache = BlockDirectory.NO_CACHE;
    }

    String bindAddress = configuration.get(BLUR_SHARD_BIND_ADDRESS);
    int bindPort = configuration.getInt(BLUR_SHARD_BIND_PORT, -1);
    bindPort += serverIndex;

    LOG.info("Shard Server using index [{0}] bind address [{1}]", serverIndex, bindAddress + ":" + bindPort);

    String nodeNameHostName = getNodeName(configuration, BLUR_SHARD_HOSTNAME);
    String nodeName = nodeNameHostName + ":" + bindPort;
    String zkConnectionStr = isEmpty(configuration.get(BLUR_ZOOKEEPER_CONNECTION), BLUR_ZOOKEEPER_CONNECTION);

    BlurQueryChecker queryChecker = new BlurQueryChecker(configuration);

    final ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr);
    try {
      ZookeeperSystemTime.checkSystemTime(zooKeeper, configuration.getLong(BLUR_ZOOKEEPER_SYSTEM_TIME_TOLERANCE, 3000));
    } catch (KeeperException e) {
      if (e.code() == Code.CONNECTIONLOSS) {
        System.err.println("Cannot connect zookeeper to [" + zkConnectionStr + "]");
        System.exit(1);
      }
    }
    
    BlurUtil.setupZookeeper(zooKeeper,configuration.get(BLUR_CLUSTER_NAME));

    final ZookeeperClusterStatus clusterStatus = new ZookeeperClusterStatus(zooKeeper);

    final BlurIndexRefresher refresher = new BlurIndexRefresher();
    refresher.init();

    BlurFilterCache filterCache = getFilterCache(configuration);
    BlurIndexWarmup indexWarmup = getIndexWarmup(configuration);
    IndexDeletionPolicy indexDeletionPolicy = new TimeBasedIndexDeletionPolicy(configuration.getLong(BLUR_SHARD_INDEX_DELETION_POLICY_MAXAGE, 60000));

    final DistributedIndexServer indexServer = new DistributedIndexServer();
    indexServer.setBlurMetrics(blurMetrics);
    indexServer.setCache(cache);
    indexServer.setClusterStatus(clusterStatus);
    indexServer.setConfiguration(config);
    indexServer.setNodeName(nodeName);
    indexServer.setRefresher(refresher);
    indexServer.setShardOpenerThreadCount(configuration.getInt(BLUR_SHARD_OPENER_THREAD_COUNT, 16));
    indexServer.setZookeeper(zooKeeper);
    indexServer.setFilterCache(filterCache);
    indexServer.setSafeModeDelay(configuration.getLong(BLUR_SHARD_SAFEMODEDELAY, 60000));
    indexServer.setWarmup(indexWarmup);
    indexServer.setIndexDeletionPolicy(indexDeletionPolicy);
    indexServer.setTimeBetweenCommits(configuration.getLong(BLUR_SHARD_TIME_BETWEEN_COMMITS, 60000));
    indexServer.setTimeBetweenRefreshs(configuration.getLong(BLUR_SHARD_TIME_BETWEEN_REFRESHS, 500));
    indexServer.init();

    final IndexManager indexManager = new IndexManager();
    indexManager.setIndexServer(indexServer);
    indexManager.setMaxClauseCount(configuration.getInt(BLUR_MAX_CLAUSE_COUNT, 1024));
    indexManager.setThreadCount(configuration.getInt(BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT, 32));
    indexManager.setBlurMetrics(blurMetrics);
    indexManager.setFilterCache(filterCache);
    indexManager.init();

    final BlurShardServer shardServer = new BlurShardServer();
    shardServer.setIndexServer(indexServer);
    shardServer.setIndexManager(indexManager);
    shardServer.setZookeeper(zooKeeper);
    shardServer.setClusterStatus(clusterStatus);
    shardServer.setDataFetchThreadCount(configuration.getInt(BLUR_SHARD_DATA_FETCH_THREAD_COUNT, 8));
    shardServer.setMaxQueryCacheElements(configuration.getInt(BLUR_SHARD_CACHE_MAX_QUERYCACHE_ELEMENTS, 128));
    shardServer.setMaxTimeToLive(configuration.getLong(BLUR_SHARD_CACHE_MAX_TIMETOLIVE, TimeUnit.MINUTES.toMillis(1)));
    shardServer.setQueryChecker(queryChecker);
    shardServer.init();

    Iface iface = BlurUtil.recordMethodCallsAndAverageTimes(blurMetrics, shardServer, Iface.class);

    int threadCount = configuration.getInt(BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT, 32);

    final ThriftBlurShardServer server = new ThriftBlurShardServer();
    server.setNodeName(nodeName);
    server.setBindAddress(bindAddress);
    server.setBindPort(bindPort);
    server.setThreadCount(threadCount);
    server.setIface(iface);
    server.setConfiguration(configuration);
    
    int webServerPort = Integer.parseInt(configuration.get(BLUR_GUI_SHARD_PORT)) + serverIndex;

    //TODO: this got ugly, there has to be a better way to handle all these params 
    //without reversing the mvn dependancy and making blur-gui on top. 
    final HttpJettyServer httpServer = new HttpJettyServer(bindPort, webServerPort,
    		configuration.getInt(BLUR_CONTROLLER_BIND_PORT, -1),
    		configuration.getInt(BLUR_SHARD_BIND_PORT, -1),
    		configuration.getInt(BLUR_GUI_CONTROLLER_PORT,-1),
    		configuration.getInt(BLUR_GUI_SHARD_PORT,-1),
    		"shard", blurMetrics);

    // This will shutdown the server when the correct path is set in zk
    BlurShutdown shutdown = new BlurShutdown() {
      @Override
      public void shutdown() {
        ThreadWatcher threadWatcher = ThreadWatcher.instance();
        quietClose(refresher, server, shardServer, indexManager, indexServer, threadWatcher, clusterStatus, zooKeeper, httpServer);
      }
    };
    server.setShutdown(shutdown);
    new BlurServerShutDown().register(shutdown, zooKeeper);
    return server;
  }

  private static BlurFilterCache getFilterCache(BlurConfiguration configuration) {
    String _blurFilterCacheClass = configuration.get(BLUR_SHARD_FILTER_CACHE_CLASS);
    if (_blurFilterCacheClass != null) {
      try {
        Class<?> clazz = Class.forName(_blurFilterCacheClass);
        return (BlurFilterCache) clazz.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return new DefaultBlurFilterCache();
  }

  private static BlurIndexWarmup getIndexWarmup(BlurConfiguration configuration) {
    String _blurFilterCacheClass = configuration.get(BLUR_SHARD_INDEX_WARMUP_CLASS);
    if (_blurFilterCacheClass != null) {
      try {
        Class<?> clazz = Class.forName(_blurFilterCacheClass);
        return (BlurIndexWarmup) clazz.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return new DefaultBlurIndexWarmup();
  }
}
