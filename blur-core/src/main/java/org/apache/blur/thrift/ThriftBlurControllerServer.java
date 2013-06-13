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
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_BIND_ADDRESS;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_BIND_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_CACHE_MAX_QUERYCACHE_ELEMENTS;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_CACHE_MAX_TIMETOLIVE;
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
import static org.apache.blur.utils.BlurConstants.BLUR_GUI_CONTROLLER_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_GUI_SHARD_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BIND_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static org.apache.blur.utils.BlurUtil.quietClose;

import java.util.concurrent.TimeUnit;

import org.apache.blur.BlurConfiguration;
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
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.zookeeper.ZooKeeper;


public class ThriftBlurControllerServer extends ThriftServer {

  private static final Log LOG = LogFactory.getLog(ThriftBlurControllerServer.class);

  public static void main(String[] args) throws Exception {
    int serverIndex = getServerIndex(args);
    LOG.info("Setting up Controller Server");
    BlurConfiguration configuration = new BlurConfiguration();
    printUlimits();
    ReporterSetup.setupReporters(configuration);
    ThriftServer server = createServer(serverIndex, configuration);
    server.start();
  }

  public static ThriftServer createServer(int serverIndex, BlurConfiguration configuration) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(new SimpleUncaughtExceptionHandler());
    String bindAddress = configuration.get(BLUR_CONTROLLER_BIND_ADDRESS);
    int bindPort = configuration.getInt(BLUR_CONTROLLER_BIND_PORT, -1);
    bindPort += serverIndex;

    LOG.info("Shard Server using index [{0}] bind address [{1}]", serverIndex, bindAddress + ":" + bindPort);

    String nodeName = ThriftBlurShardServer.getNodeName(configuration, BLUR_CONTROLLER_HOSTNAME);
    nodeName = nodeName + ":" + bindPort;
    String zkConnectionStr = isEmpty(configuration.get(BLUR_ZOOKEEPER_CONNECTION), BLUR_ZOOKEEPER_CONNECTION);

    BlurQueryChecker queryChecker = new BlurQueryChecker(configuration);

    final ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr);

    //@TODO this is confusing because controllers are in a cluster by default, but they see all the shards clusters.
    BlurUtil.setupZookeeper(zooKeeper, BlurConstants.BLUR_CLUSTER);

    final ZookeeperClusterStatus clusterStatus = new ZookeeperClusterStatus(zooKeeper);

    BlurControllerServer.BlurClient client = new BlurControllerServer.BlurClientRemote();

    final BlurControllerServer controllerServer = new BlurControllerServer();
    controllerServer.setClient(client);
    controllerServer.setClusterStatus(clusterStatus);
    controllerServer.setZookeeper(zooKeeper);
    controllerServer.setNodeName(nodeName);
    controllerServer.setRemoteFetchCount(configuration.getInt(BLUR_CONTROLLER_REMOTE_FETCH_COUNT, 100));
    controllerServer.setMaxQueryCacheElements(configuration.getInt(BLUR_CONTROLLER_CACHE_MAX_QUERYCACHE_ELEMENTS, 128));
    controllerServer.setMaxTimeToLive(configuration.getLong(BLUR_CONTROLLER_CACHE_MAX_TIMETOLIVE, TimeUnit.MINUTES.toMillis(1)));
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

    controllerServer.init();

    Iface iface = BlurUtil.recordMethodCallsAndAverageTimes(controllerServer, Iface.class);

    int threadCount = configuration.getInt(BLUR_CONTROLLER_SERVER_THRIFT_THREAD_COUNT, 32);

    final ThriftBlurControllerServer server = new ThriftBlurControllerServer();
    server.setNodeName(nodeName);
    server.setConfiguration(configuration);
    server.setBindAddress(bindAddress);
    server.setBindPort(bindPort);
    server.setThreadCount(threadCount);
    server.setIface(iface);

    int baseGuiPort = Integer.parseInt(configuration.get(BLUR_GUI_CONTROLLER_PORT));
    final HttpJettyServer httpServer;
    if (baseGuiPort > 0) {
      int webServerPort = baseGuiPort + serverIndex;
      // TODO: this got ugly, there has to be a better way to handle all these
      // params
      // without reversing the mvn dependancy and making blur-gui on top.
      httpServer = new HttpJettyServer(bindPort, webServerPort, configuration.getInt(BLUR_CONTROLLER_BIND_PORT, -1), configuration.getInt(BLUR_SHARD_BIND_PORT, -1),
          configuration.getInt(BLUR_GUI_CONTROLLER_PORT, -1), configuration.getInt(BLUR_GUI_SHARD_PORT, -1), "controller");
    } else {
      httpServer = null;
    }

    // This will shutdown the server when the correct path is set in zk
    BlurShutdown shutdown = new BlurShutdown() {
      @Override
      public void shutdown() {
        ThreadWatcher threadWatcher = ThreadWatcher.instance();
        quietClose(server, controllerServer, clusterStatus, zooKeeper, threadWatcher, httpServer);
      }
    };
    server.setShutdown(shutdown);
    new BlurServerShutDown().register(shutdown, zooKeeper);
    return server;
  }
}
