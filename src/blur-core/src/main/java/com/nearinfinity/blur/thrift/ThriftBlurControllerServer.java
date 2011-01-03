package com.nearinfinity.blur.thrift;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TFramedTransport.Factory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.manager.indexserver.ControllerIndexServer;
import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.thrift.client.BlurClient;
import com.nearinfinity.blur.thrift.client.BlurClientRemote;
import com.nearinfinity.blur.thrift.generated.BlurSearch;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Iface;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Processor;

public class ThriftBlurControllerServer {
    
    private static final Log LOG = LogFactory.getLog(ThriftBlurControllerServer.class);
    
    private int port = 40010;
    private Iface iface;
    
    public static void main(String[] args) throws TTransportException, IOException {
        ZooKeeper zooKeeper = new ZooKeeper("localhost", 10000, new Watcher() {
            @Override
            public void process(WatchedEvent arg0) {
            }
        });
        
        ZookeeperDistributedManager dzk = new ZookeeperDistributedManager();
        dzk.setZooKeeper(zooKeeper);
        
        ControllerIndexServer indexServer = new ControllerIndexServer();
        indexServer.setNodeName("localhost");
        indexServer.setZk(dzk);
        indexServer.init();
        
        BlurClient client = new BlurClientRemote();
        
        BlurControllerServer controllerServer = new BlurControllerServer();
        controllerServer.setClient(client);
        controllerServer.setIndexServer(indexServer);
        
        ThriftBlurControllerServer server = new ThriftBlurControllerServer();
        server.setPort(40010);
        server.setIface(controllerServer);
        server.start();
    }

    public void start() throws TTransportException {
        TServerSocket serverTransport = new TServerSocket(port);
        Factory transportFactory = new TFramedTransport.Factory();
        Processor processor = new BlurSearch.Processor(iface);
        TBinaryProtocol.Factory protFactory = new TBinaryProtocol.Factory(true, true);
        TThreadPoolServer server = new TThreadPoolServer(processor, serverTransport, transportFactory, protFactory);
        LOG.info("Starting server on port [" + port + "]");
        server.serve();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Iface getIface() {
        return iface;
    }

    public void setIface(Iface iface) {
        this.iface = iface;
    }

}
