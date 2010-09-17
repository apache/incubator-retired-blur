package com.nearinfinity.blur.thrift;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.Blur.Processor;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.mele.Mele;

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

	public static void main(String[] args) throws IOException, BlurException, InterruptedException {
		BlurConfiguration configuration = new BlurConfiguration();
		Mele mele = new Mele(configuration);
		for (String arg : args) {
		    if (SHARD.equals(arg) && shardServer != null) {
		        shardServer = new BlurThriftServer(configuration.getBlurShardServerPort(), 
		                new BlurShardServer(mele,configuration)).start(SHARD);
		        shardServer.waitForShutdown();
		    } else if (CONTROLLER.equals(arg) && controllerServer != null) {
		        controllerServer = new BlurThriftServer(configuration.getBlurControllerServerPort(), 
		                new BlurControllerServer(mele,configuration)).start(CONTROLLER);
		        controllerServer.waitForShutdown();
		    }
		}
	}
	
	public void waitForShutdown() throws InterruptedException {
        listeningThread.join();
    }

    public BlurThriftServer start(String name) {
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
	    listeningThread.setDaemon(true);
	    listeningThread.setName("Thrift Server Listener Thread - " + name);
	    listeningThread.start();
		return this;
	}
	
	public void stop() {
	    serverTransport.close();
	    server.stop();
	}

}
