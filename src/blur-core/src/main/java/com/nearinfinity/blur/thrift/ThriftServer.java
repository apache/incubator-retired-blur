package com.nearinfinity.blur.thrift;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TFramedTransport.Factory;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.Blur.Processor;

public class ThriftServer {
    
    private static final Log LOG = LogFactory.getLog(ThriftServer.class);

    private String addressPropertyName;
    private String portPropertyName;
    private String nodeName;
    private Iface iface;
    private TThreadPoolServer server;
    private boolean closed;
    private BlurConfiguration configuration;
    
    public synchronized void close() {
        if (!closed) {
            closed = true;
            server.stop();
        }
    }
    
    public void start() throws TTransportException {
        TServerSocket serverTransport = new TServerSocket(getBindInetSocketAddress(configuration));
        Factory transportFactory = new TFramedTransport.Factory();
        Processor processor = new Blur.Processor(iface);
        TBinaryProtocol.Factory protFactory = new TBinaryProtocol.Factory(true, true);
        
        Args args = new Args(serverTransport);
        args.processor(processor);
        args.protocolFactory(protFactory);
        args.transportFactory(transportFactory);
        args.minWorkerThreads = 10;
        args.maxWorkerThreads = 10;
        
        server = new TThreadPoolServer(args);
        LOG.info("Starting server [{0}]",nodeName);
        server.serve();
    }
    
    public InetSocketAddress getBindInetSocketAddress(BlurConfiguration configuration) {
        String hostName = ThriftBlurShardServer.isEmpty(configuration.get(addressPropertyName), addressPropertyName);
        String portStr = ThriftBlurShardServer.isEmpty(configuration.get(portPropertyName), portPropertyName);
        return new InetSocketAddress(hostName, Integer.parseInt(portStr));
    }
    
    public static String isEmpty(String str, String name) {
        if (str == null || str.trim().isEmpty()) {
            throw new IllegalArgumentException("Property [" + name + "] is missing or blank.");
        }
        return str;
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

    public void setAddressPropertyName(String addressPropertyName) {
        this.addressPropertyName = addressPropertyName;
    }

    public void setPortPropertyName(String portPropertyName) {
        this.portPropertyName = portPropertyName;
    }
    
    public static String getNodeName(BlurConfiguration configuration, String hostNameProperty) throws UnknownHostException {
        String hostName = configuration.get(hostNameProperty);
        if (hostName == null) {
            hostName = "";
        }
        hostName = hostName.trim();
        if (hostName.isEmpty()) {
            return InetAddress.getLocalHost().getHostName();
        }
        return hostName;
    }

}
