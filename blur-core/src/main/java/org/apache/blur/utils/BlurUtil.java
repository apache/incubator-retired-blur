package org.apache.blur.utils;

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
import static org.apache.blur.metrics.MetricsConstants.BLUR;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.THRIFT_CALLS;
import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_FILTERED_SERVER_CLASS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_FILTERED_SERVER_CLASS;
import static org.apache.blur.utils.BlurConstants.SHARD_PREFIX;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.regex.Pattern;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.clusterstatus.ZookeeperPathConstants;
import org.apache.blur.manager.results.BlurResultComparator;
import org.apache.blur.manager.results.BlurResultIterable;
import org.apache.blur.manager.results.BlurResultPeekableIteratorComparator;
import org.apache.blur.manager.results.PeekableIterator;
import org.apache.blur.server.BlurServerContext;
import org.apache.blur.server.ControllerServerContext;
import org.apache.blur.server.FilteredBlurServer;
import org.apache.blur.server.ShardServerContext;
import org.apache.blur.thirdparty.thrift_0_9_0.TBase;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TJSONProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TMemoryBuffer;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.util.ResetableTMemoryBuffer;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

public class BlurUtil {

  private static final Log REQUEST_LOG = LogFactory.getLog("REQUEST_LOG");
  private static final Log RESPONSE_LOG = LogFactory.getLog("RESPONSE_LOG");

  private static final Object[] EMPTY_OBJECT_ARRAY = new Object[] {};
  private static final Class<?>[] EMPTY_PARAMETER_TYPES = new Class[] {};
  private static final Log LOG = LogFactory.getLog(BlurUtil.class);
  private static final String UNKNOWN = "UNKNOWN";
  private static Pattern validator = Pattern.compile("^[a-zA-Z0-9\\_\\-]+$");

  public static final Comparator<? super PeekableIterator<BlurResult, BlurException>> HITS_PEEKABLE_ITERATOR_COMPARATOR = new BlurResultPeekableIteratorComparator();
  public static final Comparator<? super BlurResult> HITS_COMPARATOR = new BlurResultComparator();
  public static final Term PRIME_DOC_TERM = new Term(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE);

  static class LoggerArgsState {
    public LoggerArgsState(int size) {
      _buffer = new ResetableTMemoryBuffer(size);
      _tjsonProtocol = new TJSONProtocol(_buffer);
    }

    TJSONProtocol _tjsonProtocol;
    ResetableTMemoryBuffer _buffer;
    StringBuilder _builder = new StringBuilder();
  }

  @SuppressWarnings("unchecked")
  public static <T extends Iface> T recordMethodCallsAndAverageTimes(final T t, Class<T> clazz, final boolean controller) {
    final Map<String, Histogram> histogramMap = new ConcurrentHashMap<String, Histogram>();
    Method[] declaredMethods = Iface.class.getDeclaredMethods();
    for (Method m : declaredMethods) {
      String name = m.getName();
      histogramMap.put(name, Metrics.newHistogram(new MetricName(ORG_APACHE_BLUR, BLUR, name, THRIFT_CALLS)));
    }
    final String prefix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    InvocationHandler handler = new InvocationHandler() {
      private final AtomicLong _requestCounter = new AtomicLong();
      private ThreadLocal<LoggerArgsState> _loggerArgsState = new ThreadLocal<LoggerArgsState>() {
        @Override
        protected LoggerArgsState initialValue() {
          return new LoggerArgsState(1024);
        }
      };

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        long requestNumber = _requestCounter.incrementAndGet();
        String requestId = prefix + "-" + requestNumber;
        String tracingConnectionString;
        String connectionString;
        if (controller) {
          ControllerServerContext controllerServerContext = ControllerServerContext.getControllerServerContext();
          if (controllerServerContext == null) {
            connectionString = "unknown";
            tracingConnectionString = "unknown";
          } else {
            connectionString = controllerServerContext.getConnectionString("\t");
            tracingConnectionString = controllerServerContext.getConnectionString(":");
          }
        } else {
          ShardServerContext shardServerContext = ShardServerContext.getShardServerContext();
          if (shardServerContext == null) {
            connectionString = "unknown";
            tracingConnectionString = "unknown";
          } else {
            connectionString = shardServerContext.getConnectionString("\t");
            tracingConnectionString = shardServerContext.getConnectionString(":");
          }
        }
        String argsStr = null;
        long start = System.nanoTime();
        String name = method.getName();
        boolean error = false;
        LoggerArgsState loggerArgsState = null;
        Tracer trace = Trace.trace("thrift recv", Trace.param("method", method.getName()),
            Trace.param("connection", tracingConnectionString));
        try {
          if (REQUEST_LOG.isInfoEnabled()) {
            if (argsStr == null) {
              loggerArgsState = _loggerArgsState.get();
              argsStr = getArgsStr(args, name, loggerArgsState);
            }
            REQUEST_LOG.info(requestId + "\t" + connectionString + "\t" + name + "\t" + argsStr);
          }
          return method.invoke(t, args);
        } catch (InvocationTargetException e) {
          error = true;
          throw e.getTargetException();
        } finally {
          trace.done();
          long end = System.nanoTime();
          double ms = (end - start) / 1000000.0;
          if (RESPONSE_LOG.isInfoEnabled()) {
            if (argsStr == null) {
              if (loggerArgsState == null) {
                loggerArgsState = _loggerArgsState.get();
              }
              argsStr = getArgsStr(args, name, loggerArgsState);
            }
            if (error) {
              RESPONSE_LOG.info(requestId + "\t" + connectionString + "\tERROR\t" + name + "\t" + ms + "\t" + argsStr);
            } else {
              RESPONSE_LOG
                  .info(requestId + "\t" + connectionString + "\tSUCCESS\t" + name + "\t" + ms + "\t" + argsStr);
            }
          }
          Histogram histogram = histogramMap.get(name);
          histogram.update((end - start) / 1000);
        }
      }

      private String getArgsStr(Object[] args, String name, LoggerArgsState loggerArgsState) {
        String argsStr;
        if (name.equals("mutate")) {
          RowMutation rowMutation = (RowMutation) args[0];
          if (rowMutation == null) {
            argsStr = "[null]";
          } else {
            argsStr = "[" + rowMutation.getTable() + "," + rowMutation.getRowId() + "]";
          }
        } else if (name.equals("mutateBatch")) {
          argsStr = "[Batch Update]";
        } else {
          argsStr = getArgsStr(args, loggerArgsState);
        }
        return argsStr;
      }

      private String getArgsStr(Object[] args, LoggerArgsState loggerArgsState) {
        if (args == null) {
          return null;
        }
        StringBuilder builder = loggerArgsState._builder;
        builder.setLength(0);
        for (Object o : args) {
          if (builder.length() == 0) {
            builder.append('[');
          } else {
            builder.append(',');
          }
          builder.append(getArgsStr(o, loggerArgsState));
        }
        if (builder.length() != 0) {
          builder.append(']');
        }
        return builder.toString();
      }

      @SuppressWarnings("rawtypes")
      private String getArgsStr(Object o, LoggerArgsState loggerArgsState) {
        if (o == null) {
          return null;
        }
        if (o instanceof TBase) {
          return getArgsStr((TBase) o, loggerArgsState);
        }
        return o.toString();
      }

      @SuppressWarnings("rawtypes")
      private String getArgsStr(TBase o, LoggerArgsState loggerArgsState) {
        ResetableTMemoryBuffer buffer = loggerArgsState._buffer;
        TJSONProtocol tjsonProtocol = loggerArgsState._tjsonProtocol;
        buffer.resetBuffer();
        tjsonProtocol.reset();
        try {
          o.write(tjsonProtocol);
        } catch (TException e) {
          LOG.error("Unknown error tyring to write object [{0}] to json.", e, o);
        }
        byte[] array = buffer.getArray();
        int length = buffer.length();
        return new String(array, 0, length);
      }
    };
    return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class[] { clazz }, handler);
  }

  public static void setupZookeeper(ZooKeeper zookeeper) throws KeeperException, InterruptedException {
    setupZookeeper(zookeeper, null);
  }

  public synchronized static void setupZookeeper(ZooKeeper zookeeper, String cluster) throws KeeperException,
      InterruptedException {
    BlurUtil.createIfMissing(zookeeper, ZookeeperPathConstants.getBasePath());
    BlurUtil.createIfMissing(zookeeper, ZookeeperPathConstants.getOnlineControllersPath());
    BlurUtil.createIfMissing(zookeeper, ZookeeperPathConstants.getClustersPath());
    if (cluster != null) {
      BlurUtil.createIfMissing(zookeeper, ZookeeperPathConstants.getClusterPath(cluster));
      BlurUtil.createIfMissing(zookeeper, ZookeeperPathConstants.getSafemodePath(cluster));
      BlurUtil.createIfMissing(zookeeper, ZookeeperPathConstants.getRegisteredShardsPath(cluster));
      BlurUtil.createIfMissing(zookeeper, ZookeeperPathConstants.getOnlineShardsPath(cluster));
      BlurUtil.createIfMissing(zookeeper, ZookeeperPathConstants.getTablesPath(cluster));
    }
  }

  public static void createIfMissing(ZooKeeper zookeeper, String path) throws KeeperException, InterruptedException {
    if (zookeeper.exists(path, false) == null) {
      try {
        zookeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        if (e.code() == Code.NODEEXISTS) {
          return;
        }
        throw e;
      }
    }
  }

  public static List<Long> getList(AtomicLongArray atomicLongArray) {
    if (atomicLongArray == null) {
      return null;
    }
    List<Long> counts = new ArrayList<Long>(atomicLongArray.length());
    for (int i = 0; i < atomicLongArray.length(); i++) {
      counts.add(atomicLongArray.get(i));
    }
    return counts;
  }

  public static void quietClose(Object... close) {
    if (close == null) {
      return;
    }
    for (Object object : close) {
      if (object != null) {
        close(object);
      }
    }
  }

  private static void close(Object object) {
    Class<? extends Object> clazz = object.getClass();
    Method method;
    try {
      method = clazz.getMethod("close", EMPTY_PARAMETER_TYPES);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      return;
    }
    try {
      method.invoke(object, EMPTY_OBJECT_ARRAY);
    } catch (Exception e) {
      LOG.error("Error while trying to close object [{0}]", e, object);
    }
  }

  public static byte[] toBytes(Serializable serializable) {
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      ObjectOutputStream stream = new ObjectOutputStream(outputStream);
      stream.writeObject(serializable);
      stream.close();
      return outputStream.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Serializable fromBytes(byte[] bs) {
    ObjectInputStream stream = null;
    try {
      stream = new ObjectInputStream(new ByteArrayInputStream(bs));
      return (Serializable) stream.readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
          // eat
        }
      }
    }
  }

  public static List<Long> toList(AtomicLongArray atomicLongArray) {
    if (atomicLongArray == null) {
      return null;
    }
    int length = atomicLongArray.length();
    List<Long> result = new ArrayList<Long>(length);
    for (int i = 0; i < length; i++) {
      result.add(atomicLongArray.get(i));
    }
    return result;
  }

  public static AtomicLongArray getAtomicLongArraySameLengthAsList(List<?> list) {
    if (list == null) {
      return null;
    }
    return new AtomicLongArray(list.size());
  }

  public static BlurResults convertToHits(BlurResultIterable hitsIterable, BlurQuery query,
      AtomicLongArray facetCounts, ExecutorService executor, Selector selector, final Iface iface, final String table)
      throws InterruptedException, ExecutionException, BlurException {
    BlurResults results = new BlurResults();
    results.setTotalResults(hitsIterable.getTotalResults());
    results.setShardInfo(hitsIterable.getShardInfo());
    if (query.minimumNumberOfResults > 0) {
      hitsIterable.skipTo(query.start);
      int count = 0;
      BlurIterator<BlurResult, BlurException> iterator = hitsIterable.iterator();
      while (iterator.hasNext() && count < query.fetch) {
        results.addToResults(iterator.next());
        count++;
      }
    }
    if (results.results == null) {
      results.results = new ArrayList<BlurResult>();
    }
    if (facetCounts != null) {
      results.facetCounts = BlurUtil.toList(facetCounts);
    }
    if (selector != null) {
      List<Future<FetchResult>> futures = new ArrayList<Future<FetchResult>>();
      for (int i = 0; i < results.results.size(); i++) {
        BlurResult result = results.results.get(i);
        final Selector s = new Selector(selector);
        s.setLocationId(result.locationId);
        futures.add(executor.submit(new Callable<FetchResult>() {
          @Override
          public FetchResult call() throws Exception {
            return iface.fetchRow(table, s);
          }
        }));
      }
      for (int i = 0; i < results.results.size(); i++) {
        Future<FetchResult> future = futures.get(i);
        BlurResult result = results.results.get(i);
        result.setFetchResult(future.get());
      }
    }
    results.query = query;
    results.query.selector = selector;
    return results;
  }

  public static void setStartTime(BlurQuery query) {
    if (query.startTime == 0) {
      query.startTime = System.currentTimeMillis();
    }
  }

  public static String getVersion() {
    String path = "/META-INF/maven/org.apache.blur/blur-core/pom.properties";
    InputStream inputStream = BlurUtil.class.getResourceAsStream(path);
    if (inputStream == null) {
      return UNKNOWN;
    }
    Properties prop = new Properties();
    try {
      prop.load(inputStream);
    } catch (IOException e) {
      LOG.error("Unknown error while getting version.", e);
      return UNKNOWN;
    }
    Object verison = prop.get("version");
    if (verison == null) {
      return UNKNOWN;
    }
    return verison.toString();
  }

  public static void unlockForSafeMode(ZooKeeper zookeeper, String lockPath) throws InterruptedException,
      KeeperException {
    zookeeper.delete(lockPath, -1);
    LOG.info("Lock released.");
  }

  public static String lockForSafeMode(ZooKeeper zookeeper, String nodeName, String cluster) throws KeeperException,
      InterruptedException {
    LOG.info("Getting safe mode lock.");
    final Object lock = new Object();
    String blurSafemodePath = ZookeeperPathConstants.getSafemodePath(cluster);
    String newPath = zookeeper.create(blurSafemodePath + "/safemode-", nodeName.getBytes(), Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL);
    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        synchronized (lock) {
          lock.notifyAll();
        }
      }
    };
    while (true) {
      synchronized (lock) {
        List<String> children = new ArrayList<String>(zookeeper.getChildren(blurSafemodePath, watcher));
        Collections.sort(children);
        if (newPath.equals(blurSafemodePath + "/" + children.get(0))) {
          LOG.info("Lock aquired.");
          return newPath;
        } else {
          lock.wait(BlurConstants.ZK_WAIT_TIME);
        }
      }
    }
  }

  public static String getShardName(int id) {
    return getShardName(BlurConstants.SHARD_PREFIX, id);
  }

  public static String getShardName(String prefix, int id) {
    return prefix + buffer(id, 8);
  }

  private static String buffer(int value, int length) {
    String str = Integer.toString(value);
    while (str.length() < length) {
      str = "0" + str;
    }
    return str;
  }

  public static String humanizeTime(long time, TimeUnit unit) {
    long seconds = unit.toSeconds(time);
    long hours = getHours(seconds);
    seconds = seconds - TimeUnit.HOURS.toSeconds(hours);
    long minutes = getMinutes(seconds);
    seconds = seconds - TimeUnit.MINUTES.toSeconds(minutes);
    return humanizeTime(hours, minutes, seconds);
  }

  public static String humanizeTime(long hours, long minutes, long seconds) {
    StringBuilder builder = new StringBuilder();
    if (hours == 0 && minutes != 0) {
      addMinutes(builder, minutes);
    } else if (hours != 0) {
      addHours(builder, hours);
      addMinutes(builder, minutes);
    }
    addSeconds(builder, seconds);
    return builder.toString().trim();
  }

  private static void addHours(StringBuilder builder, long hours) {
    builder.append(hours).append(" hours ");
  }

  private static void addMinutes(StringBuilder builder, long minutes) {
    builder.append(minutes).append(" minutes ");
  }

  private static void addSeconds(StringBuilder builder, long seconds) {
    builder.append(seconds).append(" seconds ");
  }

  private static long getMinutes(long seconds) {
    return seconds / TimeUnit.MINUTES.toSeconds(1);
  }

  private static long getHours(long seconds) {
    return seconds / TimeUnit.HOURS.toSeconds(1);
  }

  public static void createPath(ZooKeeper zookeeper, String path, byte[] data) throws KeeperException,
      InterruptedException {
    zookeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  public static void setupFileSystem(String uri, int shardCount) throws IOException {
    Path tablePath = new Path(uri);
    FileSystem fileSystem = FileSystem.get(tablePath.toUri(), new Configuration());
    if (createPath(fileSystem, tablePath)) {
      LOG.info("Table uri existed.");
      validateShardCount(shardCount, fileSystem, tablePath);
    }
    for (int i = 0; i < shardCount; i++) {
      String shardName = BlurUtil.getShardName(SHARD_PREFIX, i);
      Path shardPath = new Path(tablePath, shardName);
      createPath(fileSystem, shardPath);
    }
  }

  public static void validateShardCount(int shardCount, FileSystem fileSystem, Path tablePath) throws IOException {
    // Check that all the directories that should be are in fact there.
    for (int i = 0; i < shardCount; i++) {
      Path path = new Path(tablePath, BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, i));
      if (!fileSystem.exists(path)) {
        LOG.error("Path [{0}] for shard [{1}] does not exist.", path, i);
        throw new RuntimeException("Path [" + path + "] for shard [" + i + "] does not exist.");
      }
    }

    FileStatus[] listStatus = fileSystem.listStatus(tablePath);
    for (FileStatus fs : listStatus) {
      Path path = fs.getPath();
      String name = path.getName();
      if (name.startsWith(SHARD_PREFIX)) {
        int index = name.indexOf('-');
        String shardIndexStr = name.substring(index + 1);
        int shardIndex = Integer.parseInt(shardIndexStr);
        if (shardIndex >= shardCount) {
          LOG.error("Number of directories in table path [" + path + "] exceeds definition of [" + shardCount
              + "] shard count.");
          throw new RuntimeException("Number of directories in table path [" + path + "] exceeds definition of ["
              + shardCount + "] shard count.");
        }
      }
    }
  }

  public static boolean createPath(FileSystem fileSystem, Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      LOG.info("Path [{0}] does not exist, creating.", path);
      if (!fileSystem.mkdirs(path)) {
        LOG.error("Path [{0}] was NOT created, make sure that you have correct permissions.", path);
        throw new IOException("Path [{0}] was NOT created, make sure that you have correct permissions.");
      }
      return false;
    }
    return true;
  }

  public static int zeroCheck(int i, String message) {
    if (i < 1) {
      throw new RuntimeException(message);
    }
    return i;
  }

  public static <T> T nullCheck(T t, String message) {
    if (t == null) {
      throw new NullPointerException(message);
    }
    return t;
  }

  @SuppressWarnings("unchecked")
  public static <T> T getInstance(String className, Class<T> c) {
    Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    try {
      return (T) configure(clazz.newInstance());
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T configure(T t) {
    if (t instanceof Configurable) {
      Configurable configurable = (Configurable) t;
      configurable.setConf(new Configuration());
    }
    return t;
  }

  public static byte[] read(TBase<?, ?> base) {
    if (base == null) {
      return null;
    }
    TMemoryBuffer trans = new TMemoryBuffer(1024);
    TJSONProtocol protocol = new TJSONProtocol(trans);
    try {
      base.write(protocol);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    trans.close();
    byte[] buf = new byte[trans.length()];
    System.arraycopy(trans.getArray(), 0, buf, 0, trans.length());
    return buf;
  }

  public static void write(byte[] data, TBase<?, ?> base) {
    nullCheck(null, "Data cannot be null.");
    TMemoryBuffer trans = new TMemoryBuffer(1024);
    TJSONProtocol protocol = new TJSONProtocol(trans);
    try {
      trans.write(data);
      base.read(protocol);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    trans.close();
  }

  public static void removeAll(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {
    List<String> list = zooKeeper.getChildren(path, false);
    for (String p : list) {
      removeAll(zooKeeper, path + "/" + p);
    }
    LOG.info("Removing path [{0}]", path);
    zooKeeper.delete(path, -1);
  }

  public static void removeIndexFiles(String uri) throws IOException {
    Path tablePath = new Path(uri);
    FileSystem fileSystem = FileSystem.get(tablePath.toUri(), new Configuration());
    fileSystem.delete(tablePath, true);
  }

  public static RowMutation toRowMutation(String table, Row row) {
    RowMutation rowMutation = new RowMutation();
    rowMutation.setRowId(row.getId());
    rowMutation.setTable(table);
    rowMutation.setRowMutationType(RowMutationType.REPLACE_ROW);
    List<Record> records = row.getRecords();
    for (Record record : records) {
      rowMutation.addToRecordMutations(toRecordMutation(record));
    }
    return rowMutation;
  }

  public static RecordMutation toRecordMutation(Record record) {
    RecordMutation recordMutation = new RecordMutation();
    recordMutation.setRecord(record);
    recordMutation.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);
    return recordMutation;
  }

  public static int countDocuments(IndexReader reader, Term term) throws IOException {
    TermQuery query = new TermQuery(term);
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    TopDocs topDocs = indexSearcher.search(query, 1);
    return topDocs.totalHits;
  }

  /**
   * NOTE: This is a potentially dangerous call, it will return all the
   * documents that match the term.
   * 
   * @param selector
   * 
   * @throws IOException
   */
  public static List<Document> fetchDocuments(IndexReader reader, Term term,
      ResetableDocumentStoredFieldVisitor fieldSelector, Selector selector, int maxHeap, String context)
      throws IOException {
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    int docFreq = reader.docFreq(term);

    int start = selector.getStartRecord();
    int end = selector.getMaxRecordsToFetch() + start;

    // remove potential duplicates provided from the client
    List<String> families = new ArrayList<String>();
    if (selector.getColumnFamiliesToFetch() != null) {
      for (String family : selector.getColumnFamiliesToFetch()) {
        if (!families.contains(family)) {
          families.add(family);
        }
      }
    }

    List<Document> docs = new ArrayList<Document>();
    List<ScoreDoc> scoreDocs = new ArrayList<ScoreDoc>();
    int count = 0;
    int totalHits = 0;
    while (scoreDocs.size() < end) {
      Query query = getQuery(term, families, count++);
      if (query == null) {
        break;
      }
      TopDocs topDocs = indexSearcher.search(query, docFreq);
      totalHits += topDocs.totalHits;
      scoreDocs.addAll(Arrays.asList(topDocs.scoreDocs));
      topDocs = null;
    }

    int totalHeap = 0;
    for (int i = start; i < end; i++) {
      if (i >= totalHits) {
        break;
      }
      if (totalHeap >= maxHeap) {
        LOG.warn("Max heap size exceeded for this request [{0}] max [{1}] for [{2}] and rowid [{3}]", totalHeap,
            maxHeap, context, term.text());
        break;
      }
      int doc = scoreDocs.get(i).doc;
      indexSearcher.doc(doc, fieldSelector);
      docs.add(fieldSelector.getDocument());
      int heapSize = fieldSelector.getSize();
      totalHeap += heapSize;
      fieldSelector.reset();
    }
    return docs;
  }

  private static Query getQuery(Term term, List<String> families, int index) {
    BooleanQuery booleanQuery = null;
    int familySize = families.size();
    if (familySize > 0 && index < familySize) {
      booleanQuery = new BooleanQuery();
      booleanQuery.add(new TermQuery(term), BooleanClause.Occur.MUST);
      booleanQuery.add(new TermQuery(new Term(BlurConstants.FAMILY, families.get(index))), BooleanClause.Occur.MUST);
    }
    if (booleanQuery == null && index == 0) {
      return new TermQuery(term);
    }
    return booleanQuery;
  }

  public static AtomicReader getAtomicReader(IndexReader reader) throws IOException {
    return SlowCompositeReaderWrapper.wrap(reader);
  }

  public static int getShardIndex(String shard) {
    int index = shard.indexOf('-');
    return Integer.parseInt(shard.substring(index + 1));
  }

  public static void validateRowIdAndRecord(String rowId, Record record) {
    if (!validator.matcher(record.family).matches()) {
      throw new IllegalArgumentException("Invalid column family name [ " + record.family
          + " ]. It should contain only this pattern [A-Za-z0-9_-]");
    }

    for (Column column : record.getColumns()) {
      if (!validator.matcher(column.name).matches()) {
        throw new IllegalArgumentException("Invalid column name [ " + column.name
            + " ]. It should contain only this pattern [A-Za-z0-9_-]");
      }
    }
  }

  public static void validateTableName(String tableName) {
    if (!validator.matcher(tableName).matches()) {
      throw new IllegalArgumentException("Invalid table name [ " + tableName
          + " ]. It should contain only this pattern [A-Za-z0-9_-]");
    }
  }

  public static void validateShardName(String shardName) {
    if (!validator.matcher(shardName).matches()) {
      throw new IllegalArgumentException("Invalid shard name [ " + shardName
          + " ]. It should contain only this pattern [A-Za-z0-9_-]");
    }
  }

  public static String getPid() {
    return ManagementFactory.getRuntimeMXBean().getName();
  }

  public static <T, E extends Exception> BlurIterator<T, E> convert(final Iterator<T> iterator) {
    return new BlurIterator<T, E>() {

      @Override
      public boolean hasNext() throws E {
        return iterator.hasNext();
      }

      @Override
      public T next() throws E {
        return iterator.next();
      }
    };
  }

  public static void validateWritableDirectory(FileSystem fileSystem, Path tablePath) throws IOException {
    String tmpDir = UUID.randomUUID().toString();
    String tmpFile = UUID.randomUUID().toString();
    Path path = new Path(tablePath, tmpDir);
    if (!fileSystem.mkdirs(path)) {
      throw new IOException("Could not create new directory in [" + tablePath + "] ");
    }
    Path filePath = new Path(path, tmpFile);
    try {
      fileSystem.create(filePath).close();
    } catch (IOException e) {
      throw new IOException("Could not create new filr in [" + path + "] ");
    }
    fileSystem.delete(path, true);
  }

  @SuppressWarnings("unchecked")
  public static Iface wrapFilteredBlurServer(BlurConfiguration configuration, Iface iface, boolean shard) {
    String className;
    if (!shard) {
      className = configuration.get(BLUR_CONTROLLER_FILTERED_SERVER_CLASS);
    } else {
      className = configuration.get(BLUR_SHARD_FILTERED_SERVER_CLASS);
    }
    if (className != null && !className.isEmpty()) {
      try {
        Class<? extends FilteredBlurServer> clazz = (Class<? extends FilteredBlurServer>) Class.forName(className);
        Constructor<? extends FilteredBlurServer> constructor = clazz.getConstructor(new Class[] {
            BlurConfiguration.class, Iface.class, Boolean.TYPE });
        return constructor.newInstance(new Object[] { configuration, iface, shard });
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      } catch (SecurityException e) {
        throw new RuntimeException(e);
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (IllegalArgumentException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
    return iface;
  }

  public static Iface runTrace(final Iface iface, final boolean controller) {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("startTrace")) {
          try {
            return method.invoke(iface, args);
          } catch (InvocationTargetException e) {
            throw e.getTargetException();
          }
        }
        BlurServerContext context = getServerContext(controller);
        String traceId = context.getTraceId();
        if (traceId != null) {
          Trace.setupTrace(traceId);
        }
        try {
          return method.invoke(iface, args);
        } catch (InvocationTargetException e) {
          throw e.getTargetException();
        } finally {
          Trace.tearDownTrace();
          context.setTraceId(null);
        }
      }

      private BlurServerContext getServerContext(boolean controller) {
        if (controller) {
          return ControllerServerContext.getControllerServerContext();
        }
        return ShardServerContext.getShardServerContext();
      }
    };
    return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class }, handler);
  }
}
