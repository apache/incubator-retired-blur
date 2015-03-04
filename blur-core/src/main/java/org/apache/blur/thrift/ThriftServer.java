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
import static org.apache.blur.metrics.MetricsConstants.CPU_USED;
import static org.apache.blur.metrics.MetricsConstants.HEAP_USED;
import static org.apache.blur.metrics.MetricsConstants.JVM;
import static org.apache.blur.metrics.MetricsConstants.LOAD_AVERAGE;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.SYSTEM;
import static org.apache.blur.utils.BlurConstants.BLUR_HDFS_TRACE_PATH;
import static org.apache.blur.utils.BlurConstants.BLUR_HOME;
import static org.apache.blur.utils.BlurConstants.BLUR_SERVER_SECURITY_FILTER_CLASS;
import static org.apache.blur.utils.BlurConstants.BLUR_TMP_PATH;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.indexserver.BlurServerShutDown.BlurShutdown;
import org.apache.blur.server.ServerSecurityFilter;
import org.apache.blur.server.ServerSecurityFilterFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TBinaryProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TCompactProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.server.TServer;
import org.apache.blur.thirdparty.thrift_0_9_0.server.TServerEventHandler;
import org.apache.blur.thirdparty.thrift_0_9_0.server.TThreadPoolServer;
import org.apache.blur.thirdparty.thrift_0_9_0.server.TThreadPoolServer.Args;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TFramedTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TNonblockingServerSocket;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TNonblockingServerTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TServerSocket;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TServerTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.sasl.SaslHelper;
import org.apache.blur.thrift.sasl.TSaslServerTransport;
import org.apache.blur.thrift.server.TThreadedSelectorServer;
import org.apache.blur.thrift.server.TThreadedSelectorServer.Args.AcceptPolicy;
import org.apache.blur.trace.LogTraceStorage;
import org.apache.blur.trace.TraceStorage;
import org.apache.blur.trace.hdfs.HdfsTraceStorage;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

public class ThriftServer {

  private static final Log LOG = LogFactory.getLog(ThriftServer.class);

  private String _nodeName;
  private Iface _iface;
  private TServer _server;
  private boolean _closed;
  private int _threadCount;
  private BlurShutdown _shutdown;
  private ExecutorService _executorService;
  private ExecutorService _queryExexutorService;
  private ExecutorService _mutateExecutorService;
  private TServerEventHandler _eventHandler;
  private TServerTransport _serverTransport;
  private int _acceptQueueSizePerThread = 4;
  private long _maxReadBufferBytes = Long.MAX_VALUE;
  private int _selectorThreads = 2;
  private int _maxFrameSize = 16384000;
  private BlurConfiguration _configuration;

  public int getMaxFrameSize() {
    return _maxFrameSize;
  }

  public void setMaxFrameSize(int maxFrameSize) {
    _maxFrameSize = maxFrameSize;
  }

  public TServerTransport getServerTransport() {
    return _serverTransport;
  }

  public void setServerTransport(TServerTransport serverTransport) {
    _serverTransport = serverTransport;
  }

  public static String getCommandLibPath() {
    String blurHomeDir = getBlurHomeDir();
    if (blurHomeDir == null) {
      return null;
    }
    File file = new File(blurHomeDir, "commands");
    file.mkdirs();
    if (file.exists()) {
      return file.toURI().toString();
    }
    return null;
  }

  public static String getBlurHomeDir() {
    return System.getenv(BLUR_HOME);
  }

  public static File getTmpPath(BlurConfiguration configuration) throws IOException {
    File defaultTmpPath = getDefaultTmpPath(BLUR_TMP_PATH);
    String configTmpPath = configuration.get(BLUR_TMP_PATH);
    File tmpPath;
    if (!(configTmpPath == null || configTmpPath.isEmpty())) {
      tmpPath = new File(configTmpPath);
    } else {
      tmpPath = defaultTmpPath;
    }
    return tmpPath;
  }

  public static File getDefaultTmpPath(String propName) throws IOException {
    String blurHomeDir = getBlurHomeDir();
    File tmp;
    if (blurHomeDir == null) {
      tmp = getTmpDir();
      LOG.info("Attempting to use default tmp directory [{0}]", tmp);
    } else {
      tmp = new File(blurHomeDir, "tmp");
      LOG.info("Attempting to use configured tmp directory [{0}]", tmp);
      if (!tmp.exists() && !tmp.mkdirs()) {
        tmp = getTmpDir();
        LOG.info("Attempting to use default tmp directory [{0}]", tmp);
      }
    }
    if (!tmp.exists() && !tmp.mkdirs()) {
      throw new IOException("Cannot create tmp directory [" + tmp.toURI()
          + "], please create directory or configure property [" + propName + "].");
    }
    File file = new File(tmp, UUID.randomUUID().toString());
    if (!file.createNewFile()) {
      throw new IOException("Cannot create tmp file in [" + tmp.toURI() + "].");
    }
    file.delete();
    return tmp;
  }

  private static File getTmpDir() {
    return new File(System.getProperty("java.io.tmpdir"), "blur_tmp");
  }

  public static TraceStorage setupTraceStorage(BlurConfiguration configuration) throws IOException {
    String hdfsPath = configuration.get(BLUR_HDFS_TRACE_PATH);
    if (hdfsPath != null) {
      return new HdfsTraceStorage(configuration);
    } else {
      return new LogTraceStorage(configuration);
    }
  }

  public static void printUlimits() throws IOException {
    ProcessBuilder processBuilder = new ProcessBuilder("bash", "-c", "ulimit -a");
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

  public static void setupJvmMetrics() {
    final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, SYSTEM, LOAD_AVERAGE), new Gauge<Double>() {
      @Override
      public Double value() {
        return operatingSystemMXBean.getSystemLoadAverage();
      }
    });
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, JVM, HEAP_USED), new Gauge<Long>() {
      @Override
      public Long value() {
        MemoryUsage usage = memoryMXBean.getHeapMemoryUsage();
        return usage.getUsed();
      }
    });
    Method processCpuTimeMethod = null;
    for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
      if (method.getName().equals("getProcessCpuTime")) {
        method.setAccessible(true);
        processCpuTimeMethod = method;
      }
    }
    final double availableProcessors = operatingSystemMXBean.getAvailableProcessors();
    if (processCpuTimeMethod != null) {
      final Method pctm = processCpuTimeMethod;
      Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, JVM, CPU_USED), new Gauge<Double>() {
        private long start = System.nanoTime();
        private long lastCpuTime = getProcessCputTime(pctm, operatingSystemMXBean);

        @Override
        public Double value() {
          long now = System.nanoTime();
          long cpuTime = getProcessCputTime(pctm, operatingSystemMXBean);
          long time = now - start;
          long processTime = cpuTime - lastCpuTime;
          try {
            return ((processTime / (double) time) / availableProcessors) * 100.0;
          } finally {
            lastCpuTime = cpuTime;
            start = System.nanoTime();
          }
        }
      });
    }
  }

  private static long getProcessCputTime(Method processCpuTimeMethod, OperatingSystemMXBean operatingSystemMXBean) {
    try {
      return (Long) processCpuTimeMethod.invoke(operatingSystemMXBean, new Object[] {});
    } catch (Exception e) {
      LOG.error("Unknown Error", e);
      return 0;
    }
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

  public void start() throws TTransportException, IOException {
    Blur.Processor<Blur.Iface> processor = new Blur.Processor<Blur.Iface>(_iface);
    if (SaslHelper.isSaslEnabled(_configuration)) {
      _executorService = Executors.newThreadPool("thrift-processors", _threadCount, false);
      TSaslServerTransport.Factory saslTransportFactory = SaslHelper.getTSaslServerTransportFactory(_configuration);
      Args args = new TThreadPoolServer.Args(_serverTransport);
      args.executorService(_executorService);
      args.processor(processor);
      args.protocolFactory(new TCompactProtocol.Factory());
      args.transportFactory(saslTransportFactory);

      _server = new TThreadPoolServer(args);
      _server.setServerEventHandler(_eventHandler);
    } else {
      _executorService = Executors.newThreadPool("thrift-processors", _threadCount);
      TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(
          (TNonblockingServerTransport) _serverTransport);
      args.processor(processor);
      args.executorService(_executorService);
      args.transportFactory(new TFramedTransport.Factory(_maxFrameSize));
      args.protocolFactory(new TBinaryProtocol.Factory(true, true));
      args.selectorThreads = _selectorThreads;
      args.maxReadBufferBytes = _maxReadBufferBytes;
      args.acceptQueueSizePerThread(_acceptQueueSizePerThread);
      args.acceptPolicy(AcceptPolicy.FAIR_ACCEPT);

      _server = new TThreadedSelectorServer(args);
      _server.setServerEventHandler(_eventHandler);
    }

    LOG.info("Starting server [{0}]", _nodeName);
    _server.serve();
  }

  public static InetSocketAddress getBindInetSocketAddress(String bindAddress, int bindPort) {
    return new InetSocketAddress(bindAddress, bindPort);
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

  public void setThreadCount(int threadCount) {
    _threadCount = threadCount;
  }

  public BlurShutdown getShutdown() {
    return _shutdown;
  }

  public void setShutdown(BlurShutdown shutdown) {
    _shutdown = shutdown;
  }

  public TServerEventHandler getEventHandler() {
    return _eventHandler;
  }

  public void setEventHandler(TServerEventHandler eventHandler) {
    _eventHandler = eventHandler;
  }

  public int getAcceptQueueSizePerThread() {
    return _acceptQueueSizePerThread;
  }

  public void setAcceptQueueSizePerThread(int acceptQueueSizePerThread) {
    _acceptQueueSizePerThread = acceptQueueSizePerThread;
  }

  public long getMaxReadBufferBytes() {
    return _maxReadBufferBytes;
  }

  public void setMaxReadBufferBytes(long maxReadBufferBytes) {
    _maxReadBufferBytes = maxReadBufferBytes;
  }

  public int getSelectorThreads() {
    return _selectorThreads;
  }

  public void setSelectorThreads(int selectorThreads) {
    _selectorThreads = selectorThreads;
  }

  public BlurConfiguration getConfiguration() {
    return _configuration;
  }

  public void setConfiguration(BlurConfiguration configuration) {
    this._configuration = configuration;
  }

  public static TServerTransport getTServerTransport(String bindAddress, int bindPort, BlurConfiguration configuration)
      throws TTransportException {
    InetSocketAddress bindInetSocketAddress = getBindInetSocketAddress(bindAddress, bindPort);
    if (SaslHelper.isSaslEnabled(configuration)) {
      return new TServerSocket(bindInetSocketAddress);
    } else {
      return new TNonblockingServerSocket(bindInetSocketAddress);
    }
  }

  public static int getBindingPort(TServerTransport serverTransport) {
    if (serverTransport instanceof TNonblockingServerSocket) {
      TNonblockingServerSocket nonblockingServerSocket = (TNonblockingServerSocket) serverTransport;
      return nonblockingServerSocket.getServerSocket().getLocalPort();
    } else if (serverTransport instanceof TServerSocket) {
      TServerSocket serverSocket = (TServerSocket) serverTransport;
      return serverSocket.getServerSocket().getLocalPort();
    } else {
      throw new RuntimeException("Server Transport [" + serverTransport + "] not supported.");
    }
  }

  @SuppressWarnings("unchecked")
  public static List<ServerSecurityFilter> getServerSecurityList(BlurConfiguration configuration,
      ServerSecurityFilterFactory.ServerType type) {
    Map<String, String> properties = configuration.getProperties();
    Map<String, String> classMap = new TreeMap<String, String>();
    for (Entry<String, String> e : properties.entrySet()) {
      String property = e.getKey();
      String value = e.getValue();
      if (value == null || value.isEmpty()) {
        continue;
      }
      if (property.startsWith(BLUR_SERVER_SECURITY_FILTER_CLASS)) {
        classMap.put(property, value);
      }
    }
    if (classMap.isEmpty()) {
      return null;
    }
    List<ServerSecurityFilter> result = new ArrayList<ServerSecurityFilter>();
    for (Entry<String, String> entry : classMap.entrySet()) {
      String className = entry.getValue();
      try {
        LOG.info("Loading factory class [{0}]", className);
        Class<? extends ServerSecurityFilterFactory> clazz = (Class<? extends ServerSecurityFilterFactory>) Class
            .forName(className);
        ServerSecurityFilterFactory serverSecurityFactory = clazz.newInstance();
        result.add(serverSecurityFactory.getServerSecurity(type, configuration));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }
}
