package com.nearinfinity.blur.thrift;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.Blur.Processor;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.mele.Mele;
import com.nearinfinity.mele.store.util.AddressUtil;
import com.nearinfinity.mele.store.zookeeper.NoOpWatcher;

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
    private Thread listeningThread;

	public BlurThriftServer(int port, Iface iface) {
		this.port = port;
		this.iface = iface;
	}

	public static void main(String... args) throws IOException, BlurException, InterruptedException {
	    System.out.println("Using hostname [" + AddressUtil.getMyHostName() + "]");
		BlurConfiguration configuration = new BlurConfiguration();
		ZooKeeper zooKeeper = new ZooKeeper(configuration.getZooKeeperConnectionString(), 
		        configuration.getZooKeeperSessionTimeout(), new NoOpWatcher());
		Mele mele = new Mele(zooKeeper,configuration);
		for (String arg : args) {
		    if (SHARD.equals(arg) && shardServer == null) {
		        shardServer = new BlurThriftServer(configuration.getBlurShardServerPort(), 
		                new BlurShardServer(zooKeeper,mele,configuration)).start(SHARD);
		        
		    } else if (CONTROLLER.equals(arg) && controllerServer == null) {
		        controllerServer = new BlurThriftServer(configuration.getBlurControllerServerPort(), 
		                new BlurControllerServer(zooKeeper,mele,configuration)).start(CONTROLLER);
		        
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
                    processor = new Blur.Processor(iface);
                    protFactory = new TBinaryProtocol.Factory(true, true);
                    server = new TThreadPoolServer(processor, serverTransport, protFactory);
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
