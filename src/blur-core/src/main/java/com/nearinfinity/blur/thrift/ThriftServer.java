package com.nearinfinity.blur.thrift;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.indexserver.BlurServerShutDown.BlurShutdown;
import com.nearinfinity.blur.thrift.ExecutorServicePerMethodCallThriftServer.Args;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class ThriftServer {

  private static final Log LOG = LogFactory.getLog(ThriftServer.class);

  private String _nodeName;
  private Iface _iface;
  private TServer _server;
  private boolean _closed;
  private BlurConfiguration _configuration;
  private int _threadCount;
  private int _bindPort;
  private String _bindAddress;
  private BlurShutdown _shutdown;

  public synchronized void close() {
    if (!_closed) {
      _closed = true;
      _shutdown.shutdown();
      _server.stop();
    }
  }

  protected static int getServerIndex(String[] args) {
    for (int i = 0; i < args.length; i++) {
      if ("-s".equals(args[i])) {
        if (i + 1 < args.length) {
          return Integer.parseInt(args[i + 1]);
        }
      }
    }
    return 0;
  }

  public void start() throws TTransportException {
    Blur.Processor<Blur.Iface> processor = new Blur.Processor<Blur.Iface>(_iface);
    TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(getBindInetSocketAddress(_configuration));

    Args args = new Args(serverTransport);
    args.processor(processor);
    args.executorService(Executors.newThreadPool("thrift-processors", _threadCount));
    Map<String, ExecutorService> methodCallsToExecutorService = new HashMap<String, ExecutorService>();
    ExecutorService mutateExecutorService = Executors.newThreadPool("thrift-processors-mutate", _threadCount);
    methodCallsToExecutorService.put("mutate", mutateExecutorService);
    methodCallsToExecutorService.put("mutateBatch", mutateExecutorService);
    methodCallsToExecutorService.put("query", Executors.newThreadPool("thrift-processors-query", _threadCount));
    args.setMethodCallsToExecutorService(methodCallsToExecutorService);
    _server = new ExecutorServicePerMethodCallThriftServer(args);
    LOG.info("Starting server [{0}]", _nodeName);
    _server.serve();
  }

  public InetSocketAddress getBindInetSocketAddress(BlurConfiguration configuration) {
    return new InetSocketAddress(_bindAddress, _bindPort);
  }

  public static String isEmpty(String str, String name) {
    if (str == null || str.trim().isEmpty()) {
      throw new IllegalArgumentException("Property [" + name + "] is missing or blank.");
    }
    return str;
  }

  public Iface getIface() {
    return _iface;
  }

  public void setIface(Iface iface) {
    this._iface = iface;
  }

  public String getNodeName() {
    return _nodeName;
  }

  public void setNodeName(String nodeName) {
    this._nodeName = nodeName;
  }

  public void setConfiguration(BlurConfiguration configuration) {
    this._configuration = configuration;
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

  public void setBindPort(int bindPort) {
    _bindPort = bindPort;
  }

  public void setBindAddress(String bindAddress) {
    _bindAddress = bindAddress;
  }

  public void setThreadCount(int threadCount) {
    this._threadCount = threadCount;
  }

  public BlurShutdown getShutdown() {
    return _shutdown;
  }

  public void setShutdown(BlurShutdown shutdown) {
    this._shutdown = shutdown;
  }
}
