package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;

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

import com.nearinfinity.blur.manager.indexserver.ZookeeperClusterStatus;
import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.thrift.client.BlurClient;
import com.nearinfinity.blur.thrift.client.BlurClientRemote;
import com.nearinfinity.blur.thrift.generated.BlurSearch;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Iface;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Processor;

public class ThriftBlurControllerServer {
    
    private static final Log LOG = LogFactory.getLog(ThriftBlurControllerServer.class);
    
    private String nodeName;
    private Iface iface;
    
    public static void main(String[] args) throws TTransportException, IOException {
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Unknown error in thread [" + t + "]",e);
            }
        });
        
        String nodeName = args[0];
        String zkConnectionStr = args[1];
        boolean crazyMode = false;
        if (args.length == 3 && args[2].equals(ThriftBlurShardServer.CRAZY)) {
            crazyMode = true;
        }
        
        ZooKeeper zooKeeper = new ZooKeeper(zkConnectionStr, 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
        
        ZookeeperDistributedManager dzk = new ZookeeperDistributedManager();
        dzk.setZooKeeper(zooKeeper);
        
        ZookeeperClusterStatus clusterStatus = new ZookeeperClusterStatus();
        clusterStatus.setDistributedManager(dzk);
        clusterStatus.init();
        
        BlurClient client = new BlurClientRemote();
        
        BlurControllerServer controllerServer = new BlurControllerServer();
        controllerServer.setClient(client);
        controllerServer.setClusterStatus(clusterStatus);
        controllerServer.open();
        
        ThriftBlurControllerServer server = new ThriftBlurControllerServer();
        server.setNodeName(nodeName);
        if (crazyMode) {
            System.err.println("Crazy mode!!!!!");
            server.setIface(ThriftBlurShardServer.crazyMode(controllerServer));            
        } else {
            server.setIface(controllerServer);
        }
        server.start();
    }

    public void start() throws TTransportException {
        TServerSocket serverTransport = new TServerSocket(ThriftBlurShardServer.parse(System.getProperty(ThriftBlurShardServer.BLUR_BIND_ADDRESS, nodeName)));
        Factory transportFactory = new TFramedTransport.Factory();
        Processor processor = new BlurSearch.Processor(iface);
        TBinaryProtocol.Factory protFactory = new TBinaryProtocol.Factory(true, true);
        TThreadPoolServer server = new TThreadPoolServer(processor, serverTransport, transportFactory, protFactory);
        LOG.info("Starting server [" + nodeName + "]");
        server.serve();
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

}
