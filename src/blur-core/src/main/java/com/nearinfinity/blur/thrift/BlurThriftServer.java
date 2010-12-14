package com.nearinfinity.blur.thrift;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurSearch;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Iface;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Processor;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.mele.util.AddressUtil;
import com.nearinfinity.mele.zookeeper.NoOpWatcher;

public class BlurThriftServer implements BlurConstants {

	private static final Log LOG = LogFactory.getLog(BlurThriftServer.class);

    private static BlurThriftServer controllerServer;
    private static BlurThriftServer shardServer;
	
	private Iface iface;
	private int port;
    private TThreadPoolServer server;
    private Factory protFactory;
    private Processor processor;
    private TServerSocket serverTransport;
    private TTransportFactory transportFactory;
    private Thread listeningThread;

	public BlurThriftServer(int port, Iface iface) {
		this.port = port;
		this.iface = iface;
	}

	public static void main(String... args) throws IOException, BlurException, InterruptedException {
	    System.out.println("Using hostname [" + AddressUtil.getMyHostName() + "]");
		int sessionTimeout = 10000;
        String connectionStr = "localhost?";
        ZooKeeper zooKeeper = new ZooKeeper(connectionStr, sessionTimeout, new NoOpWatcher());
		
		IndexServer indexServer = null;
		IndexManager indexManager = new IndexManager();
		indexManager.setIndexServer(indexServer);
		
		for (String arg : args) {
		    if (SHARD.equals(arg) && shardServer == null) {
		        BlurShardServer blurShardServer = new BlurShardServer();
		        blurShardServer.setIndexManager(indexManager);
		        blurShardServer.setIndexServer(indexServer);
		        
                BlurThriftServer blurThriftServer = new BlurThriftServer(40020, blurShardServer);
                shardServer = blurThriftServer.start(SHARD);
		    } else if (CONTROLLER.equals(arg) && controllerServer == null) {
		        BlurControllerServer blurControllerServer = new BlurControllerServer();
		        blurControllerServer.setIndexServer(indexServer);
		        
                BlurThriftServer blurThriftServer = new BlurThriftServer(40010, blurControllerServer);
                controllerServer = blurThriftServer.start(CONTROLLER);
		    }
		}
		if (controllerServer != null)
		    controllerServer.waitForShutdown();
		if (shardServer != null)
		    shardServer.waitForShutdown();
		zooKeeper.close();
	}
	
	public void waitForShutdown() throws InterruptedException {
        listeningThread.join();
    }

    public BlurThriftServer start(final String name) {
	    listeningThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    serverTransport = new TServerSocket(port);
                    transportFactory = new TFramedTransport.Factory();
                    processor = new BlurSearch.Processor(iface);
                    protFactory = new TBinaryProtocol.Factory(true, true);
//                    server = new TThreadPoolServer(processor, serverTransport, protFactory);
                    server = new TThreadPoolServer(processor, serverTransport, transportFactory, protFactory);
                    LOG.info("Starting server on port [" + port + "]");
                    server.serve();
                } catch (TTransportException e) {
                    LOG.error("Unknown error",e);
                }
            }
        });
	    listeningThread.setName("Thrift Server Listener Thread - " + name);
	    listeningThread.start();
	    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Stoping thrift server " + name);
                stop();
            }
        }));
		return this;
	}
	
	public void stop() {
	    serverTransport.close();
	    server.stop();
	}

}
