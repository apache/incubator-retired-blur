package org.apache.blur.thrift;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.indexserver.BlurServerShutDown.BlurShutdown;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TBinaryProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.server.TServer;
import org.apache.blur.thirdparty.thrift_0_9_0.server.TServerEventHandler;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TFramedTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TNonblockingServerSocket;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.server.TThreadedSelectorServer;

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
  private ExecutorService _executorService;
  private ExecutorService _queryExexutorService;
  private ExecutorService _mutateExecutorService;
  private TServerEventHandler _eventHandler;

  public static void printUlimits() throws IOException {
    ProcessBuilder processBuilder = new ProcessBuilder("ulimit", "-a");
    Process process;
    try {
      process = processBuilder.start();
    } catch (Exception e) {
      LOG.warn("Could not run ulimit command to retrieve limits.", e);
      return;
    }

    InputStream inputStream = process.getInputStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    while ((line = reader.readLine()) != null) {
      LOG.info("ulimit: " + line);
    }
    reader.close();
  }

  public synchronized void close() {
    if (!_closed) {
      _closed = true;
      _shutdown.shutdown();
      _server.stop();
      if (_executorService != null) {
        _executorService.shutdownNow();
      }
      if (_queryExexutorService != null) {
        _queryExexutorService.shutdownNow();
      }
      if (_mutateExecutorService != null) {
        _mutateExecutorService.shutdownNow();
      }
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
    _executorService = Executors.newThreadPool("thrift-processors", _threadCount);
    Blur.Processor<Blur.Iface> processor = new Blur.Processor<Blur.Iface>(_iface);

    TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(getBindInetSocketAddress(_configuration));
    TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(serverTransport);
    args.processor(processor);
    args.executorService(_executorService);
    args.transportFactory(new TFramedTransport.Factory());
    args.protocolFactory(new TBinaryProtocol.Factory(true, true));
    _server = new TThreadedSelectorServer(args);
    _server.setServerEventHandler(_eventHandler);
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

  public static String getNodeName(BlurConfiguration configuration, String hostNameProperty)
      throws UnknownHostException {
    String hostName = configuration.get(hostNameProperty);
    if (hostName == null) {
      hostName = "";
    }
    hostName = hostName.trim();
    if (hostName.isEmpty()) {
      try {
        return InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        String message = e.getMessage();
        int index = message.indexOf(':');
        if (index < 0) {
          throw new RuntimeException("Nodename cannot be determined.");
        }
        String nodeName = message.substring(0, index);
        LOG.warn("Hack to get nodename from exception [" + nodeName + "]");
        return nodeName;
      }
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

  public TServerEventHandler getEventHandler() {
    return _eventHandler;
  }

  public void setEventHandler(TServerEventHandler eventHandler) {
    _eventHandler = eventHandler;
  }

}
