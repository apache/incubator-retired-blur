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
import static org.apache.blur.utils.BlurConstants.BLUR_CLUSTER;
import static org.apache.blur.utils.BlurConstants.BLUR_CLUSTER_NAME;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_BIND_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_GUI_CONTROLLER_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_GUI_SHARD_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_INDEXMANAGER_MUTATE_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_MAX_CLAUSE_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_MAX_HEAP_PER_ROW_FETCH;
import static org.apache.blur.utils.BlurConstants.BLUR_MAX_RECORDS_PER_ROW_FETCH_REQUEST;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BIND_ADDRESS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BIND_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCKCACHE_DIRECT_MEMORY_ALLOCATION;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCKCACHE_SLAB_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_EXPERIMENTAL_BLOCK_CACHE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_FETCHCOUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_FILTER_CACHE_CLASS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_HOSTNAME;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_WARMUP_CLASS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_WARMUP_THROTTLE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_OPENER_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_SAFEMODEDELAY;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_THRIFT_ACCEPT_QUEUE_SIZE_PER_THREAD;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_THRIFT_MAX_READ_BUFFER_BYTES;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_THRIFT_SELECTOR_THREADS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_WARMUP_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TIMEOUT;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TIMEOUT_DEFAULT;
import static org.apache.blur.utils.BlurUtil.quietClose;

import java.util.concurrent.TimeUnit;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.concurrent.SimpleUncaughtExceptionHandler;
import org.apache.blur.concurrent.ThreadWatcher;
import org.apache.blur.gui.HttpJettyServer;
import org.apache.blur.gui.JSONReporterServlet;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.BlurFilterCache;
import org.apache.blur.manager.BlurQueryChecker;
import org.apache.blur.manager.DefaultBlurFilterCache;
import org.apache.blur.manager.IndexManager;
import org.apache.blur.manager.clusterstatus.ZookeeperClusterStatus;
import org.apache.blur.manager.indexserver.BlurIndexWarmup;
import org.apache.blur.manager.indexserver.BlurServerShutDown;
import org.apache.blur.manager.indexserver.BlurServerShutDown.BlurShutdown;
import org.apache.blur.manager.indexserver.DefaultBlurIndexWarmup;
import org.apache.blur.manager.indexserver.DistributedIndexServer;
import org.apache.blur.manager.indexserver.DistributedLayoutFactory;
import org.apache.blur.manager.indexserver.DistributedLayoutFactoryImpl;
import org.apache.blur.manager.writer.BlurIndexRefresher;
import org.apache.blur.metrics.JSONReporter;
import org.apache.blur.metrics.ReporterSetup;
import org.apache.blur.server.ShardServerEventHandler;
import org.apache.blur.server.TableContext;
import org.apache.blur.store.BlockCacheDirectoryFactory;
import org.apache.blur.store.BlockCacheDirectoryFactoryV1;
import org.apache.blur.store.BlockCacheDirectoryFactoryV2;
import org.apache.blur.store.blockcache.BlockCache;
import org.apache.blur.store.blockcache.BlockDirectory;
import org.apache.blur.store.blockcache.BlockDirectoryCache;
import org.apache.blur.store.blockcache.Cache;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TJSONProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.server.TServlet;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TNonblockingServerSocket;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.GCWatcher;
import org.apache.blur.utils.MemoryReporter;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.ZooKeeper;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;

import sun.misc.VM;

public class ThriftBlurShardServer extends ThriftServer {

  private static final Log LOG = LogFactory.getLog(ThriftBlurShardServer.class);
  private static final boolean enableJsonReporter = false;
  private static final long _64MB = 64 * 1024 * 1024;

  public static void main(String[] args) throws Exception {
    int serverIndex = getServerIndex(args);
    LOG.info("Setting up Shard Server");
    Thread.setDefaultUncaughtExceptionHandler(new SimpleUncaughtExceptionHandler());
    BlurConfiguration configuration = new BlurConfiguration();
    printUlimits();
    ReporterSetup.setupReporters(configuration);
    MemoryReporter.enable();
    setupJvmMetrics();
    // make this configurable
    GCWatcher.init(0.75);
    ThriftServer server = createServer(serverIndex, configuration, false);
    server.start();
  }

  public static ThriftServer createServer(int serverIndex, BlurConfiguration configuration, boolean randomPort)
      throws Exception {
    Configuration config = new Configuration();
    TableContext.setSystemBlurConfiguration(configuration);
    TableContext.setSystemConfiguration(config);

    String bindAddress = configuration.get(BLUR_SHARD_BIND_ADDRESS);
    int bindPort = configuration.getInt(BLUR_SHARD_BIND_PORT, -1);
    bindPort += serverIndex;
    if (randomPort) {
      bindPort = 0;
    }
    TNonblockingServerSocket tNonblockingServerSocket = ThriftServer.getTNonblockingServerSocket(bindAddress, bindPort);
    if (randomPort) {
      bindPort = tNonblockingServerSocket.getServerSocket().getLocalPort();
    }

    int baseGuiPort = Integer.parseInt(configuration.get(BLUR_GUI_SHARD_PORT));
    final HttpJettyServer httpServer;
    if (baseGuiPort > 0) {
      int webServerPort = baseGuiPort + serverIndex;

      // TODO: this got ugly, there has to be a better way to handle all these
      // params without reversing the mvn dependancy and making blur-gui on top.
      httpServer = new HttpJettyServer(bindPort, webServerPort, configuration.getInt(BLUR_CONTROLLER_BIND_PORT, -1),
          configuration.getInt(BLUR_SHARD_BIND_PORT, -1), configuration.getInt(BLUR_GUI_CONTROLLER_PORT, -1),
          configuration.getInt(BLUR_GUI_SHARD_PORT, -1), "shard");
    } else {
      httpServer = null;
    }

    int _1024Size = configuration.getInt("blur.shard.buffercache.1024", 8192);
    int _8192Size = configuration.getInt("blur.shard.buffercache.8192", 8192);
    BufferStore.init(_1024Size, _8192Size);

    BlockCacheDirectoryFactory blockCacheDirectoryFactory;
    // Alternate BlockCacheDirectoryFactory support currently disabled in 0.2.0,
    // look for it in 0.2.1
    boolean experimentalBlockCache = configuration.getBoolean(BLUR_SHARD_EXPERIMENTAL_BLOCK_CACHE, false);
    experimentalBlockCache = false;
    if (!experimentalBlockCache) {
      // setup block cache
      // 134,217,728 is the slab size, therefore there are 16,384 blocks
      // in a slab when using a block size of 8,192
      int numberOfBlocksPerSlab = 16384;
      int blockSize = BlockDirectory.BLOCK_SIZE;
      int slabCount = configuration.getInt(BLUR_SHARD_BLOCKCACHE_SLAB_COUNT, -1);
      slabCount = getSlabCount(slabCount, numberOfBlocksPerSlab, blockSize);
      Cache cache;
      if (slabCount >= 1) {
        BlockCache blockCache;
        boolean directAllocation = configuration.getBoolean(BLUR_SHARD_BLOCKCACHE_DIRECT_MEMORY_ALLOCATION, true);

        int slabSize = numberOfBlocksPerSlab * blockSize;
        LOG.info("Number of slabs of block cache [{0}] with direct memory allocation set to [{1}]", slabCount,
            directAllocation);
        LOG.info("Block cache target memory usage, slab size of [{0}] will allocate [{1}] slabs and use ~[{2}] bytes",
            slabSize, slabCount, ((long) slabCount * (long) slabSize));

        try {
          long totalMemory = (long) slabCount * (long) numberOfBlocksPerSlab * (long) blockSize;
          blockCache = new BlockCache(directAllocation, totalMemory, slabSize);
        } catch (OutOfMemoryError e) {
          if ("Direct buffer memory".equals(e.getMessage())) {
            System.err
                .println("The max direct memory is too low.  Either increase by setting (-XX:MaxDirectMemorySize=<size>g -XX:+UseLargePages) or disable direct allocation by (blur.shard.blockcache.direct.memory.allocation=false) in blur-site.properties");
            System.exit(1);
          }
          throw e;
        }
        cache = new BlockDirectoryCache(blockCache);
      } else {
        cache = BlockDirectory.NO_CACHE;
      }
      blockCacheDirectoryFactory = new BlockCacheDirectoryFactoryV1(cache);
    } else {
      long totalNumberOfBytes = VM.maxDirectMemory() - _64MB;
      blockCacheDirectoryFactory = new BlockCacheDirectoryFactoryV2(configuration, totalNumberOfBytes);
    }

    LOG.info("Shard Server using index [{0}] bind address [{1}] random port assignment [{2}]", serverIndex, bindAddress
        + ":" + bindPort, randomPort);

    String nodeNameHostName = getNodeName(configuration, BLUR_SHARD_HOSTNAME);
    String nodeName = nodeNameHostName + ":" + bindPort;
    String zkConnectionStr = isEmpty(configuration.get(BLUR_ZOOKEEPER_CONNECTION), BLUR_ZOOKEEPER_CONNECTION);

    BlurQueryChecker queryChecker = new BlurQueryChecker(configuration);

    int sessionTimeout = configuration.getInt(BLUR_ZOOKEEPER_TIMEOUT, BLUR_ZOOKEEPER_TIMEOUT_DEFAULT);

    final ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr, sessionTimeout);

    String cluster = configuration.get(BLUR_CLUSTER_NAME, BLUR_CLUSTER);
    BlurUtil.setupZookeeper(zooKeeper, cluster);

    final ZookeeperClusterStatus clusterStatus = new ZookeeperClusterStatus(zooKeeper, configuration);

    final BlurIndexRefresher refresher = new BlurIndexRefresher();
    refresher.init();

    BlurFilterCache filterCache = getFilterCache(configuration);
    BlurIndexWarmup indexWarmup = getIndexWarmup(configuration);

    DistributedLayoutFactory distributedLayoutFactory = DistributedLayoutFactoryImpl.getDistributedLayoutFactory(
        configuration, cluster, zooKeeper);

    long safeModeDelay = configuration.getLong(BLUR_SHARD_SAFEMODEDELAY, 60000);
    int shardOpenerThreadCount = configuration.getInt(BLUR_SHARD_OPENER_THREAD_COUNT, 16);
    int internalSearchThreads = configuration.getInt(BLUR_SHARD_WARMUP_THREAD_COUNT, 16);
    int warmupThreads = configuration.getInt(BLUR_SHARD_WARMUP_THREAD_COUNT, 16);

    final DistributedIndexServer indexServer = new DistributedIndexServer(config, zooKeeper, clusterStatus,
        indexWarmup, filterCache, blockCacheDirectoryFactory, distributedLayoutFactory, cluster, nodeName,
        safeModeDelay, shardOpenerThreadCount, internalSearchThreads, warmupThreads);

    final IndexManager indexManager = new IndexManager();
    indexManager.setIndexServer(indexServer);
    indexManager.setMaxClauseCount(configuration.getInt(BLUR_MAX_CLAUSE_COUNT, 1024));
    indexManager.setThreadCount(configuration.getInt(BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT, 32));
    indexManager.setMutateThreadCount(configuration.getInt(BLUR_INDEXMANAGER_MUTATE_THREAD_COUNT, 32));
    indexManager.setFilterCache(filterCache);
    indexManager.setClusterStatus(clusterStatus);
    indexManager.setFetchCount(configuration.getInt(BLUR_SHARD_FETCHCOUNT, 100));
    indexManager.setMaxHeapPerRowFetch(configuration.getInt(BLUR_MAX_HEAP_PER_ROW_FETCH, 10000000));
    indexManager.init();

    final BlurShardServer shardServer = new BlurShardServer();
    shardServer.setIndexServer(indexServer);
    shardServer.setIndexManager(indexManager);
    shardServer.setZookeeper(zooKeeper);
    shardServer.setClusterStatus(clusterStatus);
    shardServer.setQueryChecker(queryChecker);
    shardServer.setConfiguration(configuration);
    shardServer.setMaxRecordsPerRowFetchRequest(configuration.getInt(BLUR_MAX_RECORDS_PER_ROW_FETCH_REQUEST, 1000));
    shardServer.setConfiguration(configuration);
    shardServer.init();

    Iface iface = BlurUtil.recordMethodCallsAndAverageTimes(shardServer, Iface.class, false);
    if (httpServer != null) {
      WebAppContext context = httpServer.getContext();
      context.addServlet(new ServletHolder(new TServlet(new Blur.Processor<Blur.Iface>(iface),
          new TJSONProtocol.Factory())), "/blur");
      context.addServlet(new ServletHolder(new JSONReporterServlet()), "/livemetrics");
      if (enableJsonReporter) {
        JSONReporter.enable("json-reporter", 1, TimeUnit.SECONDS, 60);
      }
    }

    int threadCount = configuration.getInt(BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT, 32);

    ShardServerEventHandler eventHandler = new ShardServerEventHandler();

    final ThriftBlurShardServer server = new ThriftBlurShardServer();
    server.setNodeName(nodeName);
    server.setServerTransport(tNonblockingServerSocket);
    server.setThreadCount(threadCount);
    server.setIface(iface);
    server.setEventHandler(eventHandler);
    server.setAcceptQueueSizePerThread(configuration.getInt(BLUR_SHARD_THRIFT_ACCEPT_QUEUE_SIZE_PER_THREAD, 4));
    server.setMaxReadBufferBytes(configuration.getLong(BLUR_SHARD_THRIFT_MAX_READ_BUFFER_BYTES, Long.MAX_VALUE));
    server.setSelectorThreads(configuration.getInt(BLUR_SHARD_THRIFT_SELECTOR_THREADS, 2));

    // This will shutdown the server when the correct path is set in zk
    BlurShutdown shutdown = new BlurShutdown() {
      @Override
      public void shutdown() {
        ThreadWatcher threadWatcher = ThreadWatcher.instance();
        quietClose(refresher, server, shardServer, indexManager, indexServer, threadWatcher, clusterStatus, zooKeeper,
            httpServer);
      }
    };
    server.setShutdown(shutdown);
    new BlurServerShutDown().register(shutdown, zooKeeper);
    return server;
  }

  private static int getSlabCount(int slabCount, int numberOfBlocksPerSlab, int blockSize) {
    if (slabCount < 0) {
      long slabSize = numberOfBlocksPerSlab * blockSize;
      long maxDirectMemorySize = VM.maxDirectMemory() - _64MB;
      if (maxDirectMemorySize < slabSize) {
        throw new RuntimeException("Auto slab setup cannot happen, JVM option -XX:MaxDirectMemorySize not set.");
      }
      return (int) (maxDirectMemorySize / slabSize);
    }
    return slabCount;
  }

  private static BlurFilterCache getFilterCache(BlurConfiguration configuration) {
    String blurFilterCacheClass = configuration.get(BLUR_SHARD_FILTER_CACHE_CLASS);
    if (blurFilterCacheClass != null) {
      try {
        Class<?> clazz = Class.forName(blurFilterCacheClass);
        return (BlurFilterCache) clazz.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return new DefaultBlurFilterCache();
  }

  private static BlurIndexWarmup getIndexWarmup(BlurConfiguration configuration) {
    String blurFilterCacheClass = configuration.get(BLUR_SHARD_INDEX_WARMUP_CLASS);
    if (blurFilterCacheClass != null && blurFilterCacheClass.isEmpty()) {
      if (!blurFilterCacheClass.equals("org.apache.blur.manager.indexserver.DefaultBlurIndexWarmup")) {
        try {
          Class<?> clazz = Class.forName(blurFilterCacheClass);
          return (BlurIndexWarmup) clazz.newInstance();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    long totalThrottle = configuration.getLong(BLUR_SHARD_INDEX_WARMUP_THROTTLE, 30000000);
    int totalThreadCount = configuration.getInt(BLUR_SHARD_WARMUP_THREAD_COUNT, 30000000);
    long warmupBandwidthThrottleBytesPerSec = totalThrottle / totalThreadCount;
    if (warmupBandwidthThrottleBytesPerSec <= 0) {
      LOG.warn("Invalid values of either [{0} = {1}] or [{2} = {3}], needs to be greater then 0",
          BLUR_SHARD_INDEX_WARMUP_THROTTLE, totalThrottle, BLUR_SHARD_WARMUP_THREAD_COUNT, totalThreadCount);
    }
    return new DefaultBlurIndexWarmup(warmupBandwidthThrottleBytesPerSec);
  }
}
