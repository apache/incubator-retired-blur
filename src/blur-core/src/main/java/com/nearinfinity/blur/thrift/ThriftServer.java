package com.nearinfinity.blur.thrift;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class ThriftServer {
    
    private static final Log LOG = LogFactory.getLog(ThriftServer.class);

    private String addressPropertyName;
    private String portPropertyName;
    private String nodeName;
    private Iface iface;
    private THsHaServer server;
    private boolean closed;
    private BlurConfiguration configuration;
    private int threadCount;
    
    public synchronized void close() {
        if (!closed) {
            closed = true;
            server.stop();
        }
    }
    
    public void start() throws TTransportException {
        Blur.Processor<Blur.Iface> processor = new Blur.Processor<Blur.Iface>(iface);
        TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(getBindInetSocketAddress(configuration));
        
        Args args = new Args(serverTransport);
        args.processor(processor);
        args.executorService(Executors.newThreadPool("thrift-processors", threadCount));
        
        server = new THsHaServer(args);
        LOG.info("Starting server [{0}]",nodeName);
        server.serve();
    }

    public InetSocketAddress getBindInetSocketAddress(BlurConfiguration configuration) {
        String hostName = isEmpty(configuration.get(addressPropertyName), addressPropertyName);
        String portStr = isEmpty(configuration.get(portPropertyName), portPropertyName);
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

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
}
