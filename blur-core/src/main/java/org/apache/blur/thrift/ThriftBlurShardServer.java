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
import static org.apache.blur.utils.BlurConstants.BLUR_INDEXMANAGER_FACET_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_INDEXMANAGER_MUTATE_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_MAX_CLAUSE_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_MAX_HEAP_PER_ROW_FETCH;
import static org.apache.blur.utils.BlurConstants.BLUR_MAX_RECORDS_PER_ROW_FETCH_REQUEST;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BIND_ADDRESS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BIND_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_TOTAL_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_VERSION;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_FETCHCOUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_FILTER_CACHE_CLASS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_HOSTNAME;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_MERGE_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_OPENER_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_SAFEMODEDELAY;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_THRIFT_ACCEPT_QUEUE_SIZE_PER_THREAD;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_THRIFT_MAX_READ_BUFFER_BYTES;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_THRIFT_SELECTOR_THREADS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_WARMUP_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_THRIFT_MAX_FRAME_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TIMEOUT;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TIMEOUT_DEFAULT;
import static org.apache.blur.utils.BlurUtil.quietClose;

import java.lang.reflect.Constructor;
import java.util.Map.Entry;
import java.util.Set;
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
import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TJSONProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.server.TServlet;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TNonblockingServerSocket;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.TraceStorage;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.GCWatcher;
import org.apache.blur.utils.MemoryReporter;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.search.BooleanQuery;
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

    Set<Entry<String, String>> set = configuration.getProperties().entrySet();
    for (Entry<String, String> e : set) {
      String key = e.getKey();
      if (key.startsWith("blur.shard.buffercache.")) {
        int index = key.lastIndexOf('.');
        int bufferSize = Integer.parseInt(key.substring(index + 1));
        long amount = Long.parseLong(e.getValue());
        BufferStore.initNewBuffer(bufferSize, amount);
      }
    }

    BlockCacheDirectoryFactory blockCacheDirectoryFactory;
    // Alternate BlockCacheDirectoryFactory support currently disabled in 0.2.0,
    // look for it in 0.2.1
    String blockCacheVersion = configuration.get(BLUR_SHARD_BLOCK_CACHE_VERSION, "v2");
    long totalNumberOfBytes = configuration.getLong(BLUR_SHARD_BLOCK_CACHE_TOTAL_SIZE, VM.maxDirectMemory() - _64MB);
    if (blockCacheVersion.equals("v1")) {
      blockCacheDirectoryFactory = new BlockCacheDirectoryFactoryV1(configuration, totalNumberOfBytes);
    } else if (blockCacheVersion.equals("v2")) {
      blockCacheDirectoryFactory = new BlockCacheDirectoryFactoryV2(configuration, totalNumberOfBytes);
    } else {
      throw new RuntimeException("Unknown block cache version [" + blockCacheVersion + "] can be [v1,v2]");
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

    BlurFilterCache filterCache = getFilterCache(configuration);
    BlurIndexWarmup indexWarmup = BlurIndexWarmup.getIndexWarmup(configuration);

    DistributedLayoutFactory distributedLayoutFactory = DistributedLayoutFactoryImpl.getDistributedLayoutFactory(
        configuration, cluster, zooKeeper);

    long safeModeDelay = configuration.getLong(BLUR_SHARD_SAFEMODEDELAY, 60000);
    int shardOpenerThreadCount = configuration.getInt(BLUR_SHARD_OPENER_THREAD_COUNT, 16);
    int internalSearchThreads = configuration.getInt(BLUR_SHARD_WARMUP_THREAD_COUNT, 16);
    int warmupThreads = configuration.getInt(BLUR_SHARD_WARMUP_THREAD_COUNT, 16);
    int maxMergeThreads = configuration.getInt(BLUR_SHARD_MERGE_THREAD_COUNT, 3);
    final DistributedIndexServer indexServer = new DistributedIndexServer(config, zooKeeper, clusterStatus,
        indexWarmup, filterCache, blockCacheDirectoryFactory, distributedLayoutFactory, cluster, nodeName,
        safeModeDelay, shardOpenerThreadCount, internalSearchThreads, warmupThreads, maxMergeThreads);

    BooleanQuery.setMaxClauseCount(configuration.getInt(BLUR_MAX_CLAUSE_COUNT, 1024));

    int maxHeapPerRowFetch = configuration.getInt(BLUR_MAX_HEAP_PER_ROW_FETCH, 10000000);
    int fetchCount = configuration.getInt(BLUR_SHARD_FETCHCOUNT, 100);
    int indexManagerThreadCount = configuration.getInt(BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT, 32);
    int mutateThreadCount = configuration.getInt(BLUR_INDEXMANAGER_MUTATE_THREAD_COUNT, 32);
    int facetThreadCount = configuration.getInt(BLUR_INDEXMANAGER_FACET_THREAD_COUNT, 16);
    long statusCleanupTimerDelay = TimeUnit.SECONDS.toMillis(10);

    final IndexManager indexManager = new IndexManager(indexServer, clusterStatus, filterCache, maxHeapPerRowFetch,
        fetchCount, indexManagerThreadCount, mutateThreadCount, statusCleanupTimerDelay, facetThreadCount);

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

    final TraceStorage traceStorage = setupTraceStorage(configuration);
    Trace.setStorage(traceStorage);
    Trace.setNodeName(nodeName);

    Iface iface = BlurUtil.wrapFilteredBlurServer(configuration, shardServer, true);
    iface = BlurUtil.recordMethodCallsAndAverageTimes(iface, Iface.class, false);
    iface = BlurUtil.runWithUser(iface, false);
    iface = BlurUtil.runTrace(iface, false);
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
    server.setMaxFrameSize(config.getInt(BLUR_THRIFT_MAX_FRAME_SIZE, 16384000));

    // This will shutdown the server when the correct path is set in zk
    BlurShutdown shutdown = new BlurShutdown() {
      @Override
      public void shutdown() {
        ThreadWatcher threadWatcher = ThreadWatcher.instance();
        quietClose(traceStorage, refresher, server, shardServer, indexManager, indexServer, threadWatcher,
            clusterStatus, zooKeeper, httpServer);
      }
    };
    server.setShutdown(shutdown);
    new BlurServerShutDown().register(shutdown, zooKeeper);
    return server;
  }

  @SuppressWarnings("unchecked")
  private static BlurFilterCache getFilterCache(BlurConfiguration configuration) {
    String blurFilterCacheClass = configuration.get(BLUR_SHARD_FILTER_CACHE_CLASS);
    if (blurFilterCacheClass != null) {
      try {
        Class<? extends BlurFilterCache> clazz = (Class<? extends BlurFilterCache>) Class.forName(blurFilterCacheClass);
        Class<?>[] types = new Class<?>[] { BlurConfiguration.class };
        Constructor<? extends BlurFilterCache> constructor = clazz.getConstructor(types);
        return constructor.newInstance(new Object[] { configuration });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return new DefaultBlurFilterCache(configuration);
  }

}
