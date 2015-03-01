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
import static org.apache.blur.utils.BlurConstants.BLUR_COMMAND_LIB_PATH;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_REMOTE_FETCH_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_GC_BACK_PRESSURE_HEAP_RATIO;
import static org.apache.blur.utils.BlurConstants.BLUR_GUI_SHARD_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_HTTP_STATUS_RUNNING_PORT;
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
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_COMMAND_DRIVER_THREADS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_COMMAND_WORKER_THREADS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_DEEP_PAGING_CACHE_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_FETCHCOUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_FILTER_CACHE_CLASS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_HOSTNAME;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INTERNAL_SEARCH_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_MERGE_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_OPENER_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_REQUEST_CACHE_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_SAFEMODEDELAY;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_SERVER_MINIMUM_BEFORE_SAFEMODE_EXIT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_SMALL_MERGE_THRESHOLD;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_THRIFT_ACCEPT_QUEUE_SIZE_PER_THREAD;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_THRIFT_MAX_READ_BUFFER_BYTES;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_THRIFT_SELECTOR_THREADS;
import static org.apache.blur.utils.BlurConstants.BLUR_THRIFT_DEFAULT_MAX_FRAME_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_THRIFT_MAX_FRAME_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_TMP_PATH;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TIMEOUT;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TIMEOUT_DEFAULT;
import static org.apache.blur.utils.BlurUtil.quietClose;

import java.io.Closeable;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.command.ShardCommandManager;
import org.apache.blur.concurrent.SimpleUncaughtExceptionHandler;
import org.apache.blur.concurrent.ThreadWatcher;
import org.apache.blur.gui.HttpJettyServer;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.DeepPagingCache;
import org.apache.blur.manager.BlurFilterCache;
import org.apache.blur.manager.BlurQueryChecker;
import org.apache.blur.manager.DefaultBlurFilterCache;
import org.apache.blur.manager.IndexManager;
import org.apache.blur.manager.clusterstatus.ClusterStatus.Action;
import org.apache.blur.manager.clusterstatus.ZookeeperClusterStatus;
import org.apache.blur.manager.indexserver.BlurServerShutDown;
import org.apache.blur.manager.indexserver.BlurServerShutDown.BlurShutdown;
import org.apache.blur.manager.indexserver.DistributedIndexServer;
import org.apache.blur.manager.indexserver.DistributedLayoutFactory;
import org.apache.blur.manager.indexserver.DistributedLayoutFactoryImpl;
import org.apache.blur.metrics.JSONReporter;
import org.apache.blur.metrics.ReporterSetup;
import org.apache.blur.server.ServerSecurityFilter;
import org.apache.blur.server.ServerSecurityFilterFactory;
import org.apache.blur.server.ServerSecurityUtil;
import org.apache.blur.server.ShardServerEventHandler;
import org.apache.blur.server.TableContext;
import org.apache.blur.server.cache.ThriftCache;
import org.apache.blur.server.cache.ThriftCacheServer;
import org.apache.blur.store.BlockCacheDirectoryFactory;
import org.apache.blur.store.BlockCacheDirectoryFactoryV1;
import org.apache.blur.store.BlockCacheDirectoryFactoryV2;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TJSONProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.server.TServlet;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TServerTransport;
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
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

import sun.misc.VM;

public class ThriftBlurShardServer extends ThriftServer {

  private static final Log LOG = LogFactory.getLog(ThriftBlurShardServer.class);
  private static final boolean enableJsonReporter = false;
  private static final long _64MB = 64 * 1024 * 1024;

  public static void main(String[] args) throws Exception {
    try {
      int serverIndex = getServerIndex(args);
      LOG.info("Setting up Shard Server");
      Thread.setDefaultUncaughtExceptionHandler(new SimpleUncaughtExceptionHandler());
      BlurConfiguration configuration = new BlurConfiguration();
      printUlimits();
      ReporterSetup.setupReporters(configuration);
      MemoryReporter.enable();
      setupJvmMetrics();
      double ratio = configuration.getDouble(BLUR_GC_BACK_PRESSURE_HEAP_RATIO, 0.75);
      GCWatcher.init(ratio);
      ThriftServer server = createServer(serverIndex, configuration);
      server.start();
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(1);
    }
  }

  public static ThriftServer createServer(int serverIndex, BlurConfiguration configuration) throws Exception {
    Configuration config = new Configuration();
    TableContext.setSystemBlurConfiguration(configuration);
    TableContext.setSystemConfiguration(config);

    String bindAddress = configuration.get(BLUR_SHARD_BIND_ADDRESS);
    int configBindPort = configuration.getInt(BLUR_SHARD_BIND_PORT, -1);
    int instanceBindPort = configBindPort + serverIndex;
    if (configBindPort == 0) {
      instanceBindPort = 0;
    }
    TServerTransport serverTransport = ThriftServer.getTServerTransport(bindAddress, instanceBindPort, configuration);
    instanceBindPort = ThriftServer.getBindingPort(serverTransport);

    Set<Entry<String, String>> set = configuration.getProperties().entrySet();
    for (Entry<String, String> e : set) {
      String key = e.getKey();
      String value = e.getValue();
      if (value == null || value.isEmpty()) {
        continue;
      }
      if (key.startsWith("blur.shard.buffercache.")) {
        int index = key.lastIndexOf('.');
        int bufferSize = Integer.parseInt(key.substring(index + 1));
        long amount = Long.parseLong(e.getValue());
        BufferStore.initNewBuffer(bufferSize, amount);
      }
    }

    final BlockCacheDirectoryFactory blockCacheDirectoryFactory;
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
    LOG.info("Shard Server using index [{0}] bind address [{1}]", serverIndex, bindAddress + ":" + instanceBindPort);

    String nodeNameHostName = getNodeName(configuration, BLUR_SHARD_HOSTNAME);
    String nodeName = nodeNameHostName + ":" + instanceBindPort;
    String zkConnectionStr = isEmpty(configuration.get(BLUR_ZOOKEEPER_CONNECTION), BLUR_ZOOKEEPER_CONNECTION);

    BlurQueryChecker queryChecker = new BlurQueryChecker(configuration);

    int sessionTimeout = configuration.getInt(BLUR_ZOOKEEPER_TIMEOUT, BLUR_ZOOKEEPER_TIMEOUT_DEFAULT);

    final ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr, sessionTimeout);

    String cluster = configuration.get(BLUR_CLUSTER_NAME, BLUR_CLUSTER);
    BlurUtil.setupZookeeper(zooKeeper, cluster);

    final ZookeeperClusterStatus clusterStatus = new ZookeeperClusterStatus(zooKeeper, configuration, config);

    BlurFilterCache filterCache = getFilterCache(configuration);

    DistributedLayoutFactory distributedLayoutFactory = DistributedLayoutFactoryImpl.getDistributedLayoutFactory(
        configuration, cluster, zooKeeper);

    long requestCacheSize = configuration.getLong(BLUR_SHARD_REQUEST_CACHE_SIZE, 10000000);
    final ThriftCache thriftCache = new ThriftCache(requestCacheSize);

    long safeModeDelay = configuration.getLong(BLUR_SHARD_SAFEMODEDELAY, 60000);
    int shardOpenerThreadCount = configuration.getInt(BLUR_SHARD_OPENER_THREAD_COUNT, 16);
    int maxMergeThreads = configuration.getInt(BLUR_SHARD_MERGE_THREAD_COUNT, 3);
    int minimumNumberOfNodesBeforeExitingSafeMode = configuration.getInt(
        BLUR_SHARD_SERVER_MINIMUM_BEFORE_SAFEMODE_EXIT, 0);
    int internalSearchThreads = configuration.getInt(BLUR_SHARD_INTERNAL_SEARCH_THREAD_COUNT, 16);
    final Timer hdfsKeyValueTimer = new Timer("HDFS KV Store", true);
    final Timer indexImporterTimer = new Timer("IndexImporter", true);
    final Timer indexBulkTimer = new Timer("BulkIndex", true);
    long smallMergeThreshold = configuration.getLong(BLUR_SHARD_SMALL_MERGE_THRESHOLD, 128 * 1000 * 1000);
    final DistributedIndexServer indexServer = new DistributedIndexServer(config, zooKeeper, clusterStatus,
        filterCache, blockCacheDirectoryFactory, distributedLayoutFactory, cluster, nodeName, safeModeDelay,
        shardOpenerThreadCount, maxMergeThreads, internalSearchThreads, minimumNumberOfNodesBeforeExitingSafeMode,
        hdfsKeyValueTimer, indexImporterTimer, smallMergeThreshold, indexBulkTimer, thriftCache);

    BooleanQuery.setMaxClauseCount(configuration.getInt(BLUR_MAX_CLAUSE_COUNT, 1024));

    int maxHeapPerRowFetch = configuration.getInt(BLUR_MAX_HEAP_PER_ROW_FETCH, 10000000);
    int remoteFetchCount = configuration.getInt(BLUR_CONTROLLER_REMOTE_FETCH_COUNT, 100);
    int fetchCount = configuration.getInt(BLUR_SHARD_FETCHCOUNT, 110);
    if (fetchCount + 1 <= remoteFetchCount) {
      LOG.warn("[" + BLUR_SHARD_FETCHCOUNT + "] [" + fetchCount + "] is equal to or less than ["
          + BLUR_CONTROLLER_REMOTE_FETCH_COUNT + "] [" + remoteFetchCount + "], should be at least 1 greater.");
    }
    int indexManagerThreadCount = configuration.getInt(BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT, 32);
    int mutateThreadCount = configuration.getInt(BLUR_INDEXMANAGER_MUTATE_THREAD_COUNT, 32);
    int facetThreadCount = configuration.getInt(BLUR_INDEXMANAGER_FACET_THREAD_COUNT, 16);
    long statusCleanupTimerDelay = TimeUnit.SECONDS.toMillis(10);
    int cacheSize = configuration.getInt(BLUR_SHARD_DEEP_PAGING_CACHE_SIZE, 1000);
    DeepPagingCache deepPagingCache = new DeepPagingCache(cacheSize);

    final IndexManager indexManager = new IndexManager(indexServer, clusterStatus, filterCache, maxHeapPerRowFetch,
        fetchCount, indexManagerThreadCount, mutateThreadCount, statusCleanupTimerDelay, facetThreadCount,
        deepPagingCache);

    File defaultTmpPath = getDefaultTmpPath(BLUR_TMP_PATH);
    String configTmpPath = configuration.get(BLUR_TMP_PATH);
    File tmpPath;
    if (!(configTmpPath == null || configTmpPath.isEmpty())) {
      tmpPath = new File(configTmpPath);
    } else {
      tmpPath = defaultTmpPath;
    }
    int numberOfShardWorkerCommandThreads = configuration.getInt(BLUR_SHARD_COMMAND_WORKER_THREADS, 16);
    int numberOfShardDriverCommandThreads = configuration.getInt(BLUR_SHARD_COMMAND_DRIVER_THREADS, 16);
    String commandPath = configuration.get(BLUR_COMMAND_LIB_PATH, getCommandLibPath());
    if (commandPath != null) {
      LOG.info("Command Path was set to [{0}].", commandPath);
    } else {
      LOG.info("Command Path was not set.");
    }
    final ShardCommandManager commandManager = new ShardCommandManager(indexServer, tmpPath, commandPath,
        numberOfShardWorkerCommandThreads, numberOfShardDriverCommandThreads, Connection.DEFAULT_TIMEOUT, config);

    clusterStatus.registerActionOnTableStateChange(new Action() {
      @Override
      public void action() {
        thriftCache.clear();
      }
    });

    final BlurShardServer shardServer = new BlurShardServer();
    shardServer.setCommandManager(commandManager);
    shardServer.setIndexServer(indexServer);
    shardServer.setIndexManager(indexManager);
    shardServer.setZookeeper(zooKeeper);
    shardServer.setClusterStatus(clusterStatus);
    shardServer.setQueryChecker(queryChecker);
    shardServer.setMaxRecordsPerRowFetchRequest(configuration.getInt(BLUR_MAX_RECORDS_PER_ROW_FETCH_REQUEST, 1000));
    shardServer.setConfiguration(configuration);
    shardServer.init();

    final TraceStorage traceStorage = setupTraceStorage(configuration);
    Trace.setStorage(traceStorage);
    Trace.setNodeName(nodeName);

    List<ServerSecurityFilter> serverSecurity = getServerSecurityList(configuration,
        ServerSecurityFilterFactory.ServerType.SHARD);

    Iface iface = new ThriftCacheServer(configuration, shardServer, indexServer, thriftCache);
    iface = BlurUtil.wrapFilteredBlurServer(configuration, iface, true);
    iface = ServerSecurityUtil.applySecurity(iface, serverSecurity, true);
    iface = BlurUtil.recordMethodCallsAndAverageTimes(iface, Iface.class, false);
    iface = BlurUtil.runWithUser(iface, false);
    iface = BlurUtil.runTrace(iface, false);
    iface = BlurUtil.lastChanceErrorHandling(iface, Iface.class);

    int configGuiPort = Integer.parseInt(configuration.get(BLUR_GUI_SHARD_PORT));
    int instanceGuiPort = configGuiPort + serverIndex;

    if (configGuiPort == 0) {
      instanceGuiPort = 0;
    }

    final HttpJettyServer httpServer;
    if (configGuiPort >= 0) {
      httpServer = new HttpJettyServer(HttpJettyServer.class, instanceGuiPort);
      int port = httpServer.getLocalPort();
      configuration.setInt(BLUR_HTTP_STATUS_RUNNING_PORT, port);
    } else {
      httpServer = null;
    }
    if (httpServer != null) {
      WebAppContext context = httpServer.getContext();
      context.addServlet(new ServletHolder(new TServlet(new Blur.Processor<Blur.Iface>(iface),
          new TJSONProtocol.Factory())), "/blur");
      if (enableJsonReporter) {
        JSONReporter.enable("json-reporter", 1, TimeUnit.SECONDS, 60);
      }
    }

    int threadCount = configuration.getInt(BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT, 32);

    ShardServerEventHandler eventHandler = new ShardServerEventHandler();

    final ThriftBlurShardServer server = new ThriftBlurShardServer();
    server.setNodeName(nodeName);
    server.setServerTransport(serverTransport);
    server.setThreadCount(threadCount);
    server.setIface(iface);
    server.setEventHandler(eventHandler);
    server.setAcceptQueueSizePerThread(configuration.getInt(BLUR_SHARD_THRIFT_ACCEPT_QUEUE_SIZE_PER_THREAD, 4));
    server.setMaxReadBufferBytes(configuration.getLong(BLUR_SHARD_THRIFT_MAX_READ_BUFFER_BYTES, Long.MAX_VALUE));
    server.setSelectorThreads(configuration.getInt(BLUR_SHARD_THRIFT_SELECTOR_THREADS, 2));
    server.setMaxFrameSize(configuration.getInt(BLUR_THRIFT_MAX_FRAME_SIZE, BLUR_THRIFT_DEFAULT_MAX_FRAME_SIZE));
    server.setConfiguration(configuration);

    // This will shutdown the server when the correct path is set in zk
    BlurShutdown shutdown = new BlurShutdown() {
      @Override
      public void shutdown() {
        ThreadWatcher threadWatcher = ThreadWatcher.instance();
        quietClose(makeCloseable(hdfsKeyValueTimer), makeCloseable(indexImporterTimer), makeCloseable(indexBulkTimer),
            blockCacheDirectoryFactory, commandManager, traceStorage, server, shardServer, indexManager, indexServer,
            threadWatcher, clusterStatus, zooKeeper, httpServer);
      }
    };
    server.setShutdown(shutdown);
    new BlurServerShutDown().register(shutdown, zooKeeper);
    return server;
  }

  protected static Closeable makeCloseable(final Timer timer) {
    return new CloseableTimer(timer);
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
