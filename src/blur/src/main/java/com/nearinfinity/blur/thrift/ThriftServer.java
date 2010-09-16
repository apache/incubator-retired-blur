package com.nearinfinity.blur.thrift;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.blur.manager.util.MeleFactory;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.Blur.Processor;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.mele.MeleConfiguration;

public class ThriftServer implements BlurConstants {

	private static final Log LOG = LogFactory.getLog(ThriftServer.class);
	
	private Iface iface;
	private int port;

    private TThreadPoolServer server;

    private Factory protFactory;

    private Processor processor;

    private TServerSocket serverTransport;

	public ThriftServer(int port, Iface iface) {
		this.port = port;
		this.iface = iface;
	}

	public static void main(String[] args) throws IOException, BlurException {
		BlurConfiguration configuration = new BlurConfiguration();
		MeleFactory.setup(new MeleConfiguration());
		int port = -1;
		Iface iface = null;
		if (args.length < 1) {
			System.err.println("Server type unknown [shard,controller]");
			System.exit(1);
		}
		if (args[0].equals(SHARD)) {
			iface = new BlurShardServer(MeleFactory.getInstance());
			port = configuration.getBlurShardServerPort();
		} else if (args[0].equals(CONTROLLER)) {
			iface = new BlurControllerServer();
			port = configuration.getBlurControllerServerPort();
		} else {
			System.err.println("Server type unknown [shard,controller]");
			System.exit(1);
		}
		ThriftServer server = new ThriftServer(port, iface);
		server.start();
	}
	
	public void start() {
		try {
			serverTransport = new TServerSocket(port);
			processor = new Blur.Processor(iface);
			protFactory = new TBinaryProtocol.Factory(true, true);
			server = new TThreadPoolServer(processor, serverTransport, protFactory);
			LOG.info("Starting server on port [" +port + "]");
			server.serve();
		} catch (TTransportException e) {
			LOG.error("Unknown error",e);
		}
	}
	
	public void stop() {
	    serverTransport.close();
	    server.stop();
	}

}
