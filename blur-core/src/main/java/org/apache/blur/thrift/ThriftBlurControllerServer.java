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
import static org.apache.blur.utils.BlurConstants.BLUR_COMMAND_LIB_PATH;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_BIND_ADDRESS;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_BIND_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_COMMAND_DRIVER_THREADS;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_COMMAND_WORKER_THREADS;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_HOSTNAME;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_REMOTE_FETCH_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_RETRY_DEFAULT_DELAY;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_RETRY_FETCH_DELAY;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_RETRY_MAX_DEFAULT_DELAY;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_RETRY_MAX_DEFAULT_RETRIES;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_RETRY_MAX_FETCH_DELAY;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_RETRY_MAX_FETCH_RETRIES;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_RETRY_MAX_MUTATE_DELAY;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_RETRY_MAX_MUTATE_RETRIES;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_RETRY_MUTATE_DELAY;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_SERVER_REMOTE_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_SERVER_THRIFT_THREAD_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_SHARD_CONNECTION_TIMEOUT;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_THRIFT_ACCEPT_QUEUE_SIZE_PER_THREAD;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_THRIFT_MAX_READ_BUFFER_BYTES;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_THRIFT_SELECTOR_THREADS;
import static org.apache.blur.utils.BlurConstants.BLUR_GC_BACK_PRESSURE_HEAP_RATIO;
import static org.apache.blur.utils.BlurConstants.BLUR_GUI_CONTROLLER_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_HTTP_STATUS_RUNNING_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_MAX_RECORDS_PER_ROW_FETCH_REQUEST;
import static org.apache.blur.utils.BlurConstants.BLUR_NODENAME;
import static org.apache.blur.utils.BlurConstants.BLUR_THRIFT_DEFAULT_MAX_FRAME_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_THRIFT_MAX_FRAME_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_TMP_PATH;
import static org.apache.blur.utils.BlurUtil.quietClose;

import java.io.File;
import java.util.List;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.command.ControllerCommandManager;
import org.apache.blur.concurrent.SimpleUncaughtExceptionHandler;
import org.apache.blur.concurrent.ThreadWatcher;
import org.apache.blur.gui.HttpJettyServer;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.BlurQueryChecker;
import org.apache.blur.manager.clusterstatus.ZookeeperClusterStatus;
import org.apache.blur.manager.indexserver.BlurServerShutDown;
import org.apache.blur.manager.indexserver.BlurServerShutDown.BlurShutdown;
import org.apache.blur.metrics.ReporterSetup;
import org.apache.blur.server.ControllerServerEventHandler;
import org.apache.blur.server.ServerSecurityFilter;
import org.apache.blur.server.ServerSecurityFilterFactory;
import org.apache.blur.server.ServerSecurityUtil;
import org.apache.blur.server.TableContext;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.ZooKeeper;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

public class ThriftBlurControllerServer extends ThriftServer {

  private static final Log LOG = LogFactory.getLog(ThriftBlurControllerServer.class);

  public static void main(String[] args) throws Exception {
    try {
      int serverIndex = getServerIndex(args);
      LOG.info("Setting up Controller Server");
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
    Configuration config = BlurUtil.newHadoopConfiguration(configuration);
    TableContext.setSystemBlurConfiguration(configuration);
    TableContext.setSystemConfiguration(config);
    
    Thread.setDefaultUncaughtExceptionHandler(new SimpleUncaughtExceptionHandler());
    String bindAddress = configuration.get(BLUR_CONTROLLER_BIND_ADDRESS);
    int configBindPort = configuration.getInt(BLUR_CONTROLLER_BIND_PORT, -1);
    int instanceBindPort = configBindPort + serverIndex;
    if (configBindPort == 0) {
      instanceBindPort = 0;
    }
    TServerTransport serverTransport = ThriftServer.getTServerTransport(bindAddress, instanceBindPort, configuration);
    instanceBindPort = ThriftServer.getBindingPort(serverTransport);

    LOG.info("Controller Server using index [{0}] bind address [{1}]", serverIndex, bindAddress + ":"
        + instanceBindPort);

    String nodeName = getNodeName(configuration, BLUR_CONTROLLER_HOSTNAME);
    nodeName = nodeName + ":" + instanceBindPort;
    configuration.set(BLUR_NODENAME, nodeName);
    

    BlurQueryChecker queryChecker = new BlurQueryChecker(configuration);

    final ZooKeeper zooKeeper = setupZookeeper(configuration, null);
    
    final ZookeeperClusterStatus clusterStatus = new ZookeeperClusterStatus(zooKeeper, configuration, config);

    int timeout = configuration.getInt(BLUR_CONTROLLER_SHARD_CONNECTION_TIMEOUT, 60000);
    BlurControllerServer.BlurClient client = new BlurControllerServer.BlurClientRemote(timeout);

    File defaultTmpPath = getDefaultTmpPath(BLUR_TMP_PATH);
    String configTmpPath = configuration.get(BLUR_TMP_PATH);
    File tmpPath;
    if (!(configTmpPath == null || configTmpPath.isEmpty())) {
      tmpPath = new File(configTmpPath);
    } else {
      tmpPath = defaultTmpPath;
    }

    int numberOfControllerWorkerCommandThreads = configuration.getInt(BLUR_CONTROLLER_COMMAND_WORKER_THREADS, 16);
    int numberOfControllerDriverCommandThreads = configuration.getInt(BLUR_CONTROLLER_COMMAND_DRIVER_THREADS, 16);
    String commandPath = configuration.get(BLUR_COMMAND_LIB_PATH, getCommandLibPath());
    if (commandPath != null) {
      LOG.info("Command Path was set to [{0}].", commandPath);
    } else {
      LOG.info("Command Path was not set.");
    }
    final ControllerCommandManager controllerCommandManager = new ControllerCommandManager(tmpPath, commandPath,
        numberOfControllerWorkerCommandThreads, numberOfControllerDriverCommandThreads, Connection.DEFAULT_TIMEOUT,
        config);

    final BlurControllerServer controllerServer = new BlurControllerServer();
    controllerServer.setClient(client);
    controllerServer.setCommandManager(controllerCommandManager);
    controllerServer.setClusterStatus(clusterStatus);
    controllerServer.setZookeeper(zooKeeper);
    controllerServer.setNodeName(nodeName);
    controllerServer.setRemoteFetchCount(configuration.getInt(BLUR_CONTROLLER_REMOTE_FETCH_COUNT, 100));
    controllerServer.setQueryChecker(queryChecker);
    controllerServer.setThreadCount(configuration.getInt(BLUR_CONTROLLER_SERVER_REMOTE_THREAD_COUNT, 64));
    controllerServer.setMaxFetchRetries(configuration.getInt(BLUR_CONTROLLER_RETRY_MAX_FETCH_RETRIES, 3));
    controllerServer.setMaxMutateRetries(configuration.getInt(BLUR_CONTROLLER_RETRY_MAX_MUTATE_RETRIES, 3));
    controllerServer.setMaxDefaultRetries(configuration.getInt(BLUR_CONTROLLER_RETRY_MAX_DEFAULT_RETRIES, 3));
    controllerServer.setFetchDelay(configuration.getInt(BLUR_CONTROLLER_RETRY_FETCH_DELAY, 500));
    controllerServer.setMutateDelay(configuration.getInt(BLUR_CONTROLLER_RETRY_MUTATE_DELAY, 500));
    controllerServer.setDefaultDelay(configuration.getInt(BLUR_CONTROLLER_RETRY_DEFAULT_DELAY, 500));
    controllerServer.setMaxFetchDelay(configuration.getInt(BLUR_CONTROLLER_RETRY_MAX_FETCH_DELAY, 2000));
    controllerServer.setMaxMutateDelay(configuration.getInt(BLUR_CONTROLLER_RETRY_MAX_MUTATE_DELAY, 2000));
    controllerServer.setMaxDefaultDelay(configuration.getInt(BLUR_CONTROLLER_RETRY_MAX_DEFAULT_DELAY, 2000));
    controllerServer
        .setMaxRecordsPerRowFetchRequest(configuration.getInt(BLUR_MAX_RECORDS_PER_ROW_FETCH_REQUEST, 1000));
    controllerServer.setConfiguration(configuration);

    controllerServer.init();

    final TraceStorage traceStorage = setupTraceStorage(configuration, config);
    Trace.setStorage(traceStorage);
    Trace.setNodeName(nodeName);

    List<ServerSecurityFilter> serverSecurity = getServerSecurityList(configuration, ServerSecurityFilterFactory.ServerType.CONTROLLER);

    Iface iface = BlurUtil.wrapFilteredBlurServer(configuration, controllerServer, false);
    iface = ServerSecurityUtil.applySecurity(iface, serverSecurity, false);
    iface = BlurUtil.recordMethodCallsAndAverageTimes(iface, Iface.class, true);
    iface = BlurUtil.runWithUser(iface, true);
    iface = BlurUtil.runTrace(iface, true);
    iface = BlurUtil.lastChanceErrorHandling(iface, Iface.class);
    int threadCount = configuration.getInt(BLUR_CONTROLLER_SERVER_THRIFT_THREAD_COUNT, 32);

    ControllerServerEventHandler eventHandler = new ControllerServerEventHandler();

    final ThriftBlurControllerServer server = new ThriftBlurControllerServer();
    server.setNodeName(nodeName);
    server.setServerTransport(serverTransport);
    server.setThreadCount(threadCount);
    server.setEventHandler(eventHandler);
    server.setIface(iface);
    server.setAcceptQueueSizePerThread(configuration.getInt(BLUR_CONTROLLER_THRIFT_ACCEPT_QUEUE_SIZE_PER_THREAD, 4));
    server.setMaxReadBufferBytes(configuration.getLong(BLUR_CONTROLLER_THRIFT_MAX_READ_BUFFER_BYTES, Long.MAX_VALUE));
    server.setSelectorThreads(configuration.getInt(BLUR_CONTROLLER_THRIFT_SELECTOR_THREADS, 2));
    server.setMaxFrameSize(configuration.getInt(BLUR_THRIFT_MAX_FRAME_SIZE, BLUR_THRIFT_DEFAULT_MAX_FRAME_SIZE));
    server.setConfiguration(configuration);

    int configGuiPort = Integer.parseInt(configuration.get(BLUR_GUI_CONTROLLER_PORT));
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
    }

    // This will shutdown the server when the correct path is set in zk
    BlurShutdown shutdown = new BlurShutdown() {
      @Override
      public void shutdown() {
        ThreadWatcher threadWatcher = ThreadWatcher.instance();
        quietClose(controllerCommandManager, traceStorage, server, controllerServer, clusterStatus, zooKeeper,
            threadWatcher, httpServer);
      }
    };
    server.setShutdown(shutdown);
    new BlurServerShutDown().register(shutdown, zooKeeper);
    return server;
  }
}
