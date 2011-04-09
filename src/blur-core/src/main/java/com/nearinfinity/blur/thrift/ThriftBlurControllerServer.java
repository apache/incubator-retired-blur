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

import static com.nearinfinity.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static com.nearinfinity.blur.utils.BlurConstants.CRAZY;
import static com.nearinfinity.blur.utils.BlurUtil.quietClose;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TFramedTransport.Factory;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.concurrent.SimpleUncaughtExceptionHandler;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.indexserver.BlurServerShutDown;
import com.nearinfinity.blur.manager.indexserver.ZookeeperClusterStatus;
import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.manager.indexserver.BlurServerShutDown.BlurShutdown;
import com.nearinfinity.blur.thrift.client.BlurClient;
import com.nearinfinity.blur.thrift.client.BlurClientRemote;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.Blur.Processor;
import com.nearinfinity.blur.zookeeper.ZkUtils;

public class ThriftBlurControllerServer extends ThriftServer {
    
    private static final String BLUR_CONTROLLER_BIND_PORT = "blur.controller.bind.port";
    private static final String BLUR_CONTROLLER_BIND_ADDRESS = "blur.controller.bind.address";
    private static final Log LOG = LogFactory.getLog(ThriftBlurControllerServer.class);
    
    private String nodeName;
    private Iface iface;
    private TThreadPoolServer server;
    private boolean closed;
    private BlurConfiguration configuration;
    
    public static void main(String[] args) throws TTransportException, IOException {
        Thread.setDefaultUncaughtExceptionHandler(new SimpleUncaughtExceptionHandler());

        BlurConfiguration configuration = new BlurConfiguration();

        String nodeName = ThriftBlurShardServer.getNodeName(configuration);
        String zkConnectionStr = ThriftBlurShardServer.isEmpty(configuration.get(BLUR_ZOOKEEPER_CONNECTION),BLUR_ZOOKEEPER_CONNECTION);
        
        boolean crazyMode = false;
        if (args.length == 1 && args[1].equals(CRAZY)) {
            crazyMode = true;
        }

        final ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr);

        ZookeeperDistributedManager dzk = new ZookeeperDistributedManager();
        dzk.setZooKeeper(zooKeeper);

        final ZookeeperClusterStatus clusterStatus = new ZookeeperClusterStatus();
        clusterStatus.setDistributedManager(dzk);
        clusterStatus.init();

        BlurClient client = new BlurClientRemote();

        final BlurControllerServer controllerServer = new BlurControllerServer();
        controllerServer.setClient(client);
        controllerServer.setClusterStatus(clusterStatus);
        controllerServer.open();

        final ThriftBlurControllerServer server = new ThriftBlurControllerServer();
        server.setNodeName(nodeName);
        if (crazyMode) {
            System.err.println("Crazy mode!!!!!");
            server.setIface(ThriftBlurShardServer.crazyMode(controllerServer));
        }
        else {
            server.setIface(controllerServer);
        }

        // This will shutdown the server when the correct path is set in zk
        new BlurServerShutDown().register(new BlurShutdown() {
            @Override
            public void shutdown() {
                quietClose(server, controllerServer, clusterStatus, zooKeeper);
                System.exit(0);
            }
        }, zooKeeper);

        server.start();
    }

    public void start() throws TTransportException {
        TServerSocket serverTransport = new TServerSocket(getBindInetSocketAddress(configuration));
        Factory transportFactory = new TFramedTransport.Factory();
        Processor processor = new Blur.Processor(iface);
        TBinaryProtocol.Factory protFactory = new TBinaryProtocol.Factory(true, true);
        server = new TThreadPoolServer(processor, serverTransport, transportFactory, protFactory);
        LOG.info("Starting server [{0}]",nodeName);
        server.serve();
    }
    
    public static InetSocketAddress getBindInetSocketAddress(BlurConfiguration configuration) {
        String hostName = ThriftBlurShardServer.isEmpty(configuration.get(BLUR_CONTROLLER_BIND_ADDRESS), BLUR_CONTROLLER_BIND_ADDRESS);
        String portStr = ThriftBlurShardServer.isEmpty(configuration.get(BLUR_CONTROLLER_BIND_PORT), BLUR_CONTROLLER_BIND_PORT);
        return new InetSocketAddress(hostName, Integer.parseInt(portStr));
    }
    
    public synchronized void close() {
        if (!closed) {
            closed = true;
            server.stop();
        }
    }

    public Iface getIface() {
        return iface;
    }

    public void setIface(Iface iface) {
        this.iface = iface;
    }
    
    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setConfiguration(BlurConfiguration configuration) {
        this.configuration = configuration;
    }

}
