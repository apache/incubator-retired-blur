package com.nearinfinity.blur.thrift;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurConfiguration;

public class ThriftServer {
	
	private static final Log LOG = LogFactory.getLog(ThriftServer.class);
	
	private Iface iface;
	private int port;

	public ThriftServer(int port, Iface iface) {
		this.port = port;
		this.iface = iface;
	}

	public static void main(String[] args) throws IOException {
		BlurConfiguration configuration = new BlurConfiguration();
		int port = -1;
		Iface iface = null;
		if (args.length < 1) {
			System.err.println("Server type unknown [shard,controller]");
			System.exit(1);
		}
		if (args[0] == "shard") {
			iface = new BlurShardServer();
			port = configuration.getInt("blur.server.shard.port",-1);
		} else if (args[0] == "controller") {
			iface = new BlurControllerServer();
			port = configuration.getInt("blur.server.controller.port",-1);
		} else {
			System.err.println("Server type unknown [shard,controller]");
			System.exit(1);
		}
		ThriftServer server = new ThriftServer(port, iface);
		server.start();
	}
	
	public void start() {
		try {
			TServerSocket serverTransport = new TServerSocket(port);
			Blur.Processor processor = new Blur.Processor(iface);
			Factory protFactory = new TBinaryProtocol.Factory(true, true);
			TServer server = new TThreadPoolServer(processor, serverTransport, protFactory);
			LOG.info("Starting server on port [" + port + "]");
			server.serve();
		} catch (TTransportException e) {
			LOG.error("Unknown error",e);
		}
	}

}
