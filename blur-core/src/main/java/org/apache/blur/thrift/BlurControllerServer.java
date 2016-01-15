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
import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.blur.command.ArgumentOverlay;
import org.apache.blur.command.BlurObject;
import org.apache.blur.command.BlurObjectSerDe;
import org.apache.blur.command.CommandUtil;
import org.apache.blur.command.ControllerCommandManager;
import org.apache.blur.command.Response;
import org.apache.blur.command.Server;
import org.apache.blur.command.Shard;
import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.BlurPartitioner;
import org.apache.blur.manager.BlurQueryChecker;
import org.apache.blur.manager.IndexManager;
import org.apache.blur.manager.indexserver.DistributedLayout;
import org.apache.blur.manager.indexserver.DistributedLayoutFactory;
import org.apache.blur.manager.indexserver.DistributedLayoutFactoryImpl;
import org.apache.blur.manager.results.BlurResultIterable;
import org.apache.blur.manager.results.BlurResultIterableClient;
import org.apache.blur.manager.results.LazyBlurResult;
import org.apache.blur.manager.results.MergerBlurResultIterable;
import org.apache.blur.manager.stats.MergerTableStats;
import org.apache.blur.manager.status.MergerQueryStatusSingle;
import org.apache.blur.server.ControllerServerContext;
import org.apache.blur.server.LayoutFactory;
import org.apache.blur.server.TableContext;
import org.apache.blur.server.TableContextFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TFramedTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TSocket;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransport;
import org.apache.blur.thrift.commands.BlurCommand;
import org.apache.blur.thrift.generated.Arguments;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.CommandDescriptor;
import org.apache.blur.thrift.generated.CommandStatus;
import org.apache.blur.thrift.generated.CommandStatusState;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.HighlightOptions;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.thrift.generated.TimeoutException;
import org.apache.blur.thrift.generated.User;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Trace.TraceId;
import org.apache.blur.trace.Tracer;
import org.apache.blur.utils.BlurExecutorCompletionService;
import org.apache.blur.utils.BlurIterator;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.ForkJoin;
import org.apache.blur.utils.ForkJoin.Merger;
import org.apache.blur.utils.ForkJoin.ParallelCall;
import org.apache.blur.utils.ShardUtil;
import org.apache.blur.zookeeper.WatchChildren;
import org.apache.blur.zookeeper.WatchChildren.OnChange;
import org.apache.blur.zookeeper.WatchNodeExistance;
import org.apache.blur.zookeeper.ZookeeperPathConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class BlurControllerServer extends TableAdmin implements Iface {

  public static abstract class BlurClient {
    public abstract <T> T execute(String node, BlurCommand<T> command, int maxRetries, long backOffTime,
        long maxBackOffTime) throws BlurException, TException, IOException;
  }

  public static class BlurClientRemote extends BlurClient {
    private int _timeout;

    public BlurClientRemote(int timeout) {
      _timeout = timeout;
    }

    @Override
    public <T> T execute(final String node, final BlurCommand<T> command, final int maxRetries, final long backOffTime,
        final long maxBackOffTime) throws BlurException, TException, IOException {
      Callable<T> callable = Trace.getCallable(new Callable<T>() {
        @Override
        public T call() throws Exception {
          Tracer trace = Trace.trace("remote call - thrift", Trace.param("node", node));
          try {
            return BlurClientManager.execute(node + "#" + _timeout, command, maxRetries, backOffTime, maxBackOffTime);
          } finally {
            trace.done();
          }
        }
      });
      try {
        return callable.call();
      } catch (BlurException e) {
        throw e;
      } catch (TException e) {
        throw e;
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new BException("Unknown error during remote call to node [" + node + "]", e);
      }
    }
  }

  private static final String CONTROLLER_THREAD_POOL = "controller-thread-pool";
  private static final Log LOG = LogFactory.getLog(BlurControllerServer.class);
  private static final Map<String, Set<String>> EMPTY_MAP = Collections
      .unmodifiableMap(new HashMap<String, Set<String>>());
  private static final Set<String> EMPTY_SET = Collections.unmodifiableSet(new HashSet<String>());

  private ExecutorService _executor;
  private AtomicReference<Map<String, Map<String, String>>> _shardServerLayout = new AtomicReference<Map<String, Map<String, String>>>(
      new HashMap<String, Map<String, String>>());
  private BlurClient _client;
  private int _threadCount = 64;
  private AtomicBoolean _closed = new AtomicBoolean();
  private Map<String, Integer> _tableShardCountMap = new ConcurrentHashMap<String, Integer>();
  private BlurPartitioner _blurPartitioner = new BlurPartitioner();
  private int _remoteFetchCount = 100;
  private BlurQueryChecker _queryChecker;
  private AtomicBoolean _running = new AtomicBoolean();
  private Map<String, DistributedLayoutFactory> _distributedLayoutFactoryMap = new ConcurrentHashMap<String, DistributedLayoutFactory>();

  private int _maxFetchRetries = 3;
  private int _maxMutateRetries = 3;
  private int _maxDefaultRetries = 3;
  private long _fetchDelay = 500;
  private long _mutateDelay = 500;
  private long _defaultDelay = 500;
  private long _maxFetchDelay = 2000;
  private long _maxMutateDelay = 2000;
  private long _maxDefaultDelay = 2000;
  private long _preconnectTime = TimeUnit.MINUTES.toMillis(1);
  private long _preconnectDelay = 15000;

  private long _defaultParallelCallTimeout = TimeUnit.MINUTES.toMillis(1);
  private WatchChildren _watchForClusters;
  private ConcurrentMap<String, WatchNodeExistance> _watchForTablesPerClusterExistance = new ConcurrentHashMap<String, WatchNodeExistance>();
  private ConcurrentMap<String, WatchNodeExistance> _watchForOnlineShardsPerClusterExistance = new ConcurrentHashMap<String, WatchNodeExistance>();
  private ConcurrentMap<String, WatchChildren> _watchForTablesPerCluster = new ConcurrentHashMap<String, WatchChildren>();
  private ConcurrentMap<String, WatchChildren> _watchForTableLayoutChanges = new ConcurrentHashMap<String, WatchChildren>();
  private ConcurrentMap<String, WatchChildren> _watchForOnlineShardsPerCluster = new ConcurrentHashMap<String, WatchChildren>();
  private Timer _preconnectTimer;
  private Timer _tableContextWarmupTimer;
  private long _tableLayoutTimeoutNanos = TimeUnit.SECONDS.toNanos(30);
  private ControllerCommandManager _commandManager;
  private BlurObjectSerDe _serDe = new BlurObjectSerDe();

  public void init() throws KeeperException, InterruptedException {
    setupZookeeper();
    registerMyself();
    _executor = Executors.newThreadPool(CONTROLLER_THREAD_POOL, _threadCount);
    _running.set(true);
    watchForClusterChanges();
    List<String> clusterList = _clusterStatus.getClusterList(false);
    for (String cluster : clusterList) {
      watchForLayoutChangeEvents(cluster);
      updateLayout(cluster);
    }
    startPreconnectTimer();
    startTableContextWarmupTimer();
  }

  private void startTableContextWarmupTimer() {
    _tableContextWarmupTimer = new Timer("controller tablecontext warmup", true);
    _tableContextWarmupTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          tableContextWarmup();
        } catch (Throwable e) {
          LOG.error("Unknown error while trying to preconnect to shard servers.", e);
        }
      }
    }, getRandomDelay(_preconnectDelay, _preconnectDelay * 4), TimeUnit.MINUTES.toMillis(1));
  }

  protected void tableContextWarmup() throws BlurException, TException {
    for (String table : tableList()) {
      LOG.debug("Warming the tablecontext for table [{0}]", table);
      getTableContext(table);
    }
  }

  private TableContext getTableContext(String table) throws BlurException, TException {
    TableDescriptor describe = describe(table);
    return TableContext.create(describe);
  }

  private long getRandomDelay(long min, long max) {
    Random random = new Random();
    return random.nextInt((int) (max - min)) + min;
  }

  private void startPreconnectTimer() {
    _preconnectTimer = new Timer("controller preconnect clients", true);
    _preconnectTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          preconnectClients();
        } catch (Throwable e) {
          LOG.error("Unknown error while trying to preconnect to shard servers.", e);
        }
      }
    }, _preconnectDelay, _preconnectTime);
  }

  private void preconnectClients() {
    if (_clusterStatus != null) {
      List<String> clusterList = _clusterStatus.getClusterList(true);
      for (String cluster : clusterList) {
        List<String> onlineShardServers = _clusterStatus.getOnlineShardServers(true, cluster);
        for (String shardServer : onlineShardServers) {
          preconnectClients(shardServer);
        }
      }
    }
  }

  private void preconnectClients(String shardServer) {
    try {
      Iface client = org.apache.blur.thrift.BlurClient.getClient(shardServer);
      client.ping();
      LOG.debug("Pinging shard server [{0}]", shardServer);
    } catch (Exception e) {
      LOG.error("Error while trying to ping shard server [{0}]", shardServer);
    }
  }

  private void setupZookeeper() throws KeeperException, InterruptedException {
    BlurUtil.createIfMissing(_zookeeper, "/blur");
    BlurUtil.createIfMissing(_zookeeper, ZookeeperPathConstants.getOnlineControllersPath());
    BlurUtil.createIfMissing(_zookeeper, ZookeeperPathConstants.getClustersPath());
  }

  private void watchForClusterChanges() throws KeeperException, InterruptedException {
    _watchForClusters = new WatchChildren(_zookeeper, ZookeeperPathConstants.getClustersPath());
    _watchForClusters.watch(new OnChange() {
      @Override
      public void action(List<String> children) {
        for (String cluster : new HashSet<String>(_distributedLayoutFactoryMap.keySet())) {
          if (!children.contains(cluster)) {
            _distributedLayoutFactoryMap.remove(cluster);
          }
        }
        for (String cluster : children) {
          try {
            watchForLayoutChangeEvents(cluster);
          } catch (KeeperException e) {
            LOG.error("Unknown error", e);
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            LOG.error("Unknown error", e);
            throw new RuntimeException(e);
          }
        }
      }
    });
  }

  private void watchForLayoutChangeEvents(final String cluster) throws KeeperException, InterruptedException {
    WatchNodeExistance we1 = new WatchNodeExistance(_zookeeper, ZookeeperPathConstants.getTablesPath(cluster));
    we1.watch(new WatchNodeExistance.OnChange() {
      @Override
      public void action(Stat stat) {
        if (stat != null) {
          watchTables(cluster, _watchForTablesPerCluster);
        }
      }
    });
    if (_watchForTablesPerClusterExistance.putIfAbsent(cluster, we1) != null) {
      we1.close();
    }
  }

  private void watchTables(final String cluster, ConcurrentMap<String, WatchChildren> map) {
    String path = ZookeeperPathConstants.getTablesPath(cluster);
    if (map.containsKey(cluster)) {
      return;
    }
    WatchChildren watchForTableLayoutChanges = new WatchChildren(_zookeeper, path);
    watchForTableLayoutChanges.watch(new OnChange() {
      @Override
      public void action(List<String> children) {
        LOG.info("Layout change for cluster [{0}].", cluster);
        updateLayout(cluster);
      }
    });
    if (map.putIfAbsent(cluster, watchForTableLayoutChanges) != null) {
      watchForTableLayoutChanges.close();
    }
  }

  private synchronized void updateLayout(String cluster) {
    if (!_clusterStatus.isOpen()) {
      LOG.warn("The cluster status object has been closed.");
      return;
    }
    List<String> tableList = _clusterStatus.getTableList(false, cluster);
    HashMap<String, Map<String, String>> newLayout = new HashMap<String, Map<String, String>>();
    for (String table : tableList) {
      watchTableLayouts(cluster, table, _watchForTableLayoutChanges);
      DistributedLayoutFactory distributedLayoutFactory = getDistributedLayoutFactory(cluster);
      DistributedLayout layout = distributedLayoutFactory.readCurrentLayout(table);
      if (layout != null) {
        Map<String, String> map = layout.getLayout();
        LOG.info("New layout for table [{0}] is [{1}]", table, map);
        newLayout.put(table, map);
      } else {
        LOG.info("Layout missing for table [{0}]", table);
      }
    }
    _shardServerLayout.set(newLayout);
  }

  private void watchTableLayouts(final String cluster, final String table, ConcurrentMap<String, WatchChildren> map) {
    String path = ZookeeperPathConstants.getTablePath(cluster, table);
    String key = cluster + "|" + table;
    if (map.containsKey(key)) {
      return;
    }
    WatchChildren watchForTableLayoutChanges = new WatchChildren(_zookeeper, path);
    watchForTableLayoutChanges.watch(new OnChange() {
      @Override
      public void action(List<String> children) {
        LOG.info("Layout change for cluster [{0}] table [{1}].", cluster, table);
        updateLayout(cluster);
      }
    });
    if (map.putIfAbsent(key, watchForTableLayoutChanges) != null) {
      watchForTableLayoutChanges.close();
    }
  }

  private synchronized DistributedLayoutFactory getDistributedLayoutFactory(String cluster) {
    DistributedLayoutFactory distributedLayoutFactory = _distributedLayoutFactoryMap.get(cluster);
    if (distributedLayoutFactory == null) {
      distributedLayoutFactory = DistributedLayoutFactoryImpl.getDistributedLayoutFactory(_configuration, cluster,
          _zookeeper);
      _distributedLayoutFactoryMap.put(cluster, distributedLayoutFactory);
    }
    return distributedLayoutFactory;
  }

  private void registerMyself() {
    // Wait for other instances (named the same name) to die
    try {
      String version = BlurUtil.getVersion();
      String onlineControllerPath = ZookeeperPathConstants.getOnlineControllersPath() + "/" + _nodeName;
      while (_zookeeper.exists(onlineControllerPath, false) != null) {
        LOG.info("Node [{0}] already registered, waiting for path [{1}] to be released", _nodeName,
            onlineControllerPath);
        Thread.sleep(3000);
      }
      _zookeeper.create(onlineControllerPath, version.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void close() {
    if (!_closed.get()) {
      _closed.set(true);
      _running.set(false);
      _executor.shutdownNow();
      _preconnectTimer.cancel();
      _preconnectTimer.purge();
      _tableContextWarmupTimer.cancel();
      _tableContextWarmupTimer.purge();
      close(_watchForClusters);
      close(_watchForOnlineShardsPerCluster.values());
      close(_watchForOnlineShardsPerClusterExistance.values());
      close(_watchForTablesPerCluster.values());
      close(_watchForTableLayoutChanges.values());
      close(_watchForTablesPerClusterExistance.values());
    }
  }

  private void close(Collection<? extends Closeable> closableLst) {
    for (Closeable closeable : closableLst) {
      close(closeable);
    }
  }

  private void close(Closeable closeable) {
    try {
      closeable.close();
    } catch (IOException e) {
      LOG.error("Unknown", e);
    }
  }

  @Override
  public BlurResults query(final String table, final BlurQuery blurQuery) throws BlurException, TException {
    try {
      checkTable(table);
      Tracer trace = Trace.trace("query - setup", Trace.param("table", table), Trace.param("blurQuery", blurQuery));
      String cluster = _clusterStatus.getCluster(true, table);
      _queryChecker.checkQuery(blurQuery);
      checkSelectorFetchSize(blurQuery.getSelector());
      TableDescriptor tableDescriptor = _clusterStatus.getTableDescriptor(true, cluster, table);
      int shardCount = tableDescriptor.getShardCount();
      if (blurQuery.getUuid() == null) {
        blurQuery.setUuid(UUID.randomUUID().toString());
      }
      BlurUtil.setStartTime(blurQuery);
      trace.done();

      BlurUtil.setStartTime(blurQuery);

      QueryCoundNotBeCompletedReport report = null;
      OUTER: for (int retries = 0; retries < _maxDefaultRetries; retries++) {
        Tracer selectorTrace = Trace.trace("selector - setup", Trace.param("retries", retries));
        final AtomicLongArray facetCounts = BlurUtil.getAtomicLongArraySameLengthAsList(blurQuery.facets);
        Selector selector = blurQuery.getSelector();
        if (selector == null) {
          selector = new Selector();
          selector.setColumnFamiliesToFetch(EMPTY_SET);
          selector.setColumnsToFetch(EMPTY_MAP);
          if (!blurQuery.query.rowQuery) {
            selector.setRecordOnly(true);
          }
        } else {
          HighlightOptions highlightOptions = selector.getHighlightOptions();
          if (highlightOptions != null && highlightOptions.getQuery() == null) {
            highlightOptions.setQuery(blurQuery.getQuery());
          }
        }
        blurQuery.setSelector(null);
        selectorTrace.done();

        BlurCommand<BlurResultIterable> command = new BlurCommand<BlurResultIterable>() {
          @Override
          public BlurResultIterable call(Client client, Connection connection) throws BlurException, TException {
            return new BlurResultIterableClient(connection, client, table, blurQuery, facetCounts, _remoteFetchCount);
          }

          @Override
          public BlurResultIterable call(Client client) throws BlurException, TException {
            throw new RuntimeException("Won't be called.");
          }
        };

        command.setDetachClient(true);

        MergerBlurResultIterable merger = new MergerBlurResultIterable(blurQuery);
        BlurResultIterable hitsIterable = null;
        try {
          String rowId = blurQuery.getRowId();
          if (rowId == null) {
            Tracer scatterGatherTrace = Trace.trace("query - scatterGather", Trace.param("retries", retries));
            try {
              hitsIterable = scatterGather(tableDescriptor.getCluster(), command, merger);
            } finally {
              scatterGatherTrace.done();
            }
          } else {
            String clientHostnamePort = getNode(table, rowId);
            hitsIterable = _client.execute(clientHostnamePort, command, _maxFetchRetries, _fetchDelay, _maxFetchDelay);
          }
          BlurResults results;
          Tracer convertToBlurResults = Trace.trace("query - convertToBlurResults", Trace.param("retries", retries));
          try {
            results = convertToBlurResults(hitsIterable, blurQuery, facetCounts, _executor, selector, table);
          } finally {
            convertToBlurResults.done();
          }
          if (!validResults(results, shardCount, blurQuery)) {
            BlurClientManager.sleep(_defaultDelay, _maxDefaultDelay, retries, _maxDefaultRetries);
            Map<String, String> map = getTableLayout(table);
            Map<String, Long> shardInfo = results.getShardInfo();
            report = new QueryCoundNotBeCompletedReport(map, shardCount, shardInfo, blurQuery);
            LOG.info("Current layout for table [{0}] is [{1}]", table, map);
            continue OUTER;
          }
          return results;
        } finally {
          if (hitsIterable != null) {
            hitsIterable.close();
          }
        }
      }
      throw new BException("Query could not be completed, report [{0}].", report);
    } catch (Exception e) {
      LOG.error("Unknown error during search of [table={0},blurQuery={1}]", e, table, blurQuery);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error during search of [table={0},blurQuery={1}]", e, table, blurQuery);
    }
  }

  public BlurResults convertToBlurResults(BlurResultIterable hitsIterable, BlurQuery query,
      AtomicLongArray facetCounts, ExecutorService executor, Selector selector, final String table)
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

      Tracer trace = Trace.trace("fetch data", Trace.param("table", table));

      // Gather client objects and build batches for fetching.
      IdentityHashMap<Client, List<Selector>> map = new IdentityHashMap<Client, List<Selector>>();

      // Need to maintain original order.
      final IdentityHashMap<Selector, Integer> indexMap = new IdentityHashMap<Selector, Integer>();
      for (int i = 0; i < results.results.size(); i++) {
        final LazyBlurResult result = (LazyBlurResult) results.results.get(i);
        Client client = result.getClient();
        Selector s = new Selector(selector);
        s.setLocationId(result.locationId);
        List<Selector> list = map.get(client);
        if (list == null) {
          list = new ArrayList<Selector>();
          map.put(client, list);
        }
        list.add(s);
        indexMap.put(s, i);
      }

      // Execute batch fetches
      List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
      final AtomicReferenceArray<FetchResult> fetchResults = new AtomicReferenceArray<FetchResult>(
          results.results.size());
      for (Entry<Client, List<Selector>> entry : map.entrySet()) {
        final Client client = entry.getKey();
        final List<Selector> list = entry.getValue();
        futures.add(executor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            Tracer trace = Trace.trace("remote call - thrift", Trace.param("node", getNode(client)));
            try {
              TraceId traceId = Trace.getTraceId();
              if (traceId != null) {
                client.startTrace(traceId.getRootId(), traceId.getRequestId());
              }
              List<FetchResult> fetchRowBatch = client.fetchRowBatch(table, list);
              for (int i = 0; i < list.size(); i++) {
                int index = indexMap.get(list.get(i));
                fetchResults.set(index, fetchRowBatch.get(i));
              }
              return Boolean.TRUE;
            } finally {
              trace.done();
            }
          }

          private String getNode(Client client) {
            TProtocol inputProtocol = client.getInputProtocol();
            TTransport transport = inputProtocol.getTransport();
            if (transport instanceof TFramedTransport) {
              TFramedTransport framedTransport = (TFramedTransport) transport;
              transport = framedTransport.getTransport();
            }
            if (transport instanceof TSocket) {
              TSocket tsocket = (TSocket) transport;
              Socket socket = tsocket.getSocket();
              SocketAddress remoteSocketAddress = socket.getRemoteSocketAddress();
              return remoteSocketAddress.toString();
            }
            return "unknown";
          }
        }));
      }

      // Wait for all parallel calls to finish.
      for (Future<Boolean> future : futures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          Throwable throwable = e.getCause();
          if (throwable instanceof BlurException) {
            throw (BlurException) throwable;
          } else {
            throw new BException("Unknown error during fetch", throwable);
          }
        }
      }

      // Place fetch results into result object for response.
      for (int i = 0; i < fetchResults.length(); i++) {
        FetchResult fetchResult = fetchResults.get(i);
        BlurResult result = results.results.get(i);
        result.setFetchResult(fetchResult);
        result.setLocationId(null);
      }

      trace.done();
    }
    results.query = query;
    results.query.selector = selector;
    return results;
  }

  private boolean validResults(BlurResults results, int shardCount, BlurQuery query) {
    if (results.totalResults >= query.minimumNumberOfResults) {
      return true;
    }
    if (query.getRowId() == null) {
      if (results.getShardInfoSize() == shardCount) {
        return true;
      }
    } else {
      if (results.getShardInfoSize() == 1) {
        return true;
      }
    }
    return false;
  }

  @Override
  public FetchResult fetchRow(final String table, final Selector selector) throws BlurException, TException {
    String clientHostnamePort = null;
    try {
      checkTable(table);
      checkSelectorFetchSize(selector);
      IndexManager.validSelector(selector);
      clientHostnamePort = getNode(table, selector);
      return _client.execute(clientHostnamePort, new BlurCommand<FetchResult>() {
        @Override
        public FetchResult call(Client client) throws BlurException, TException {
          return client.fetchRow(table, selector);
        }
      }, _maxFetchRetries, _fetchDelay, _maxFetchDelay);
    } catch (Exception e) {
      LOG.error("Unknown error during fetch of row from table [{0}] selector [{1}] node [{2}]", e, table, selector,
          clientHostnamePort);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error during fetch of row from table [{0}] selector [{1}] node [{2}]", e, table,
          selector, clientHostnamePort);
    }
  }

  @Override
  public List<FetchResult> fetchRowBatch(final String table, List<Selector> selectors) throws BlurException, TException {
    try {
      checkTable(table);
      Map<String, List<Selector>> selectorBatches = new HashMap<String, List<Selector>>();
      final Map<String, List<Integer>> selectorBatchesIndexes = new HashMap<String, List<Integer>>();
      int i = 0;
      for (Selector selector : selectors) {
        checkSelectorFetchSize(selector);
        IndexManager.validSelector(selector);
        String clientHostnamePort = getNode(table, selector);
        List<Selector> list = selectorBatches.get(clientHostnamePort);
        List<Integer> indexes = selectorBatchesIndexes.get(clientHostnamePort);
        if (list == null) {
          if (indexes != null) {
            throw new BlurException("This should never happen,", null, ErrorType.UNKNOWN);
          }
          list = new ArrayList<Selector>();
          indexes = new ArrayList<Integer>();
          selectorBatches.put(clientHostnamePort, list);
          selectorBatchesIndexes.put(clientHostnamePort, indexes);
        }
        list.add(selector);
        indexes.add(i);
        i++;
      }

      List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
      final AtomicReferenceArray<FetchResult> fetchResults = new AtomicReferenceArray<FetchResult>(new FetchResult[i]);
      for (Entry<String, List<Selector>> batch : selectorBatches.entrySet()) {
        final String clientHostnamePort = batch.getKey();
        final List<Selector> list = batch.getValue();
        futures.add(_executor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            List<FetchResult> fetchResultList = _client.execute(clientHostnamePort,
                new BlurCommand<List<FetchResult>>() {
                  @Override
                  public List<FetchResult> call(Client client) throws BlurException, TException {
                    return client.fetchRowBatch(table, list);
                  }
                }, _maxFetchRetries, _fetchDelay, _maxFetchDelay);
            List<Integer> indexes = selectorBatchesIndexes.get(clientHostnamePort);
            for (int i = 0; i < fetchResultList.size(); i++) {
              int index = indexes.get(i);
              fetchResults.set(index, fetchResultList.get(i));
            }
            return Boolean.TRUE;
          }
        }));
      }

      for (Future<Boolean> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          throw new BException("Unknown error during fetching of batch", e);
        } catch (ExecutionException e) {
          throw new BException("Unknown error during fetching of batch", e.getCause());
        }
      }

      List<FetchResult> batchResult = new ArrayList<FetchResult>();
      for (int c = 0; c < fetchResults.length(); c++) {
        FetchResult fetchResult = fetchResults.get(c);
        batchResult.add(fetchResult);
      }
      return batchResult;
    } catch (Exception e) {
      LOG.error("Unknown error during fetch a batch of rows from table [{0}]", e, table);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error during fetch a batch of rows from table [{0}]", e, table);
    }
  }

  @Override
  public void cancelQuery(final String table, final String uuid) throws BlurException, TException {
    try {
      checkTable(table);
      scatter(getCluster(table), new BlurCommand<Void>() {
        @Override
        public Void call(Client client) throws BlurException, TException {
          client.cancelQuery(table, uuid);
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to cancel search table [{0}] uuid [{1}]", e, table, uuid);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to cancel search table [{0}] uuid [{1}]", e, table, uuid);
    }
  }

  @Override
  public List<String> queryStatusIdList(final String table) throws BlurException, TException {
    try {
      checkTable(table);
      return scatterGather(getCluster(table), new BlurCommand<List<String>>() {
        @Override
        public List<String> call(Client client) throws BlurException, TException {
          return client.queryStatusIdList(table);
        }
      }, new Merger<List<String>>() {
        @Override
        public List<String> merge(BlurExecutorCompletionService<List<String>> service) throws BlurException {
          Set<String> result = new HashSet<String>();
          while (service.getRemainingCount() > 0) {
            Future<List<String>> future = service.poll(_defaultParallelCallTimeout, TimeUnit.MILLISECONDS, true);
            List<String> ids = service.getResultThrowException(future);
            result.addAll(ids);
          }
          return new ArrayList<String>(result);
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get query status ids for table [{0}]", e, table);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to get query status ids for table [{0}]", e, table);
    }
  }

  @Override
  public BlurQueryStatus queryStatusById(final String table, final String uuid) throws BlurException, TException {
    try {
      checkTable(table);
      return scatterGather(getCluster(table), new BlurCommand<BlurQueryStatus>() {
        @Override
        public BlurQueryStatus call(Client client) throws BlurException, TException {
          return client.queryStatusById(table, uuid);
        }
      }, new MergerQueryStatusSingle(_defaultParallelCallTimeout));
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get query status [{0}]", e, table, uuid);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to get query status [{0}]", e, table, uuid);
    }
  }

  @Override
  public TableStats tableStats(final String table) throws BlurException, TException {
    try {
      checkTable(table);
      return scatterGather(getCluster(table), new BlurCommand<TableStats>() {
        @Override
        public TableStats call(Client client) throws BlurException, TException {
          return client.tableStats(table);
        }
      }, new MergerTableStats(_defaultParallelCallTimeout));
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get table stats [{0}]", e, table);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to get table stats [{0}]", e, table);
    }
  }

  @Override
  public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
    try {
      checkTable(table);
      Map<String, String> tableLayout = getTableLayout(table);
      return tableLayout;
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get shard server layout [{0}]", e, table);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to get shard server layout [{0}]", e, table);
    }
  }

  @Override
  public Map<String, Map<String, ShardState>> shardServerLayoutState(final String table) throws BlurException,
      TException {
    try {
      return scatterGather(getCluster(table), new BlurCommand<Map<String, Map<String, ShardState>>>() {
        @Override
        public Map<String, Map<String, ShardState>> call(Client client) throws BlurException, TException {
          try {
            return client.shardServerLayoutState(table);
          } catch (BlurException e) {
            LOG.error("UNKOWN error from shard server", e);
            throw e;
          }
        }
      }, new Merger<Map<String, Map<String, ShardState>>>() {
        @Override
        public Map<String, Map<String, ShardState>> merge(
            BlurExecutorCompletionService<Map<String, Map<String, ShardState>>> service) throws BlurException {
          Map<String, Map<String, ShardState>> result = new HashMap<String, Map<String, ShardState>>();
          while (service.getRemainingCount() > 0) {
            Future<Map<String, Map<String, ShardState>>> future = service.poll(_defaultParallelCallTimeout,
                TimeUnit.MILLISECONDS, true, table);
            Map<String, Map<String, ShardState>> shardResult = service.getResultThrowException(future, table);
            for (Entry<String, Map<String, ShardState>> entry : shardResult.entrySet()) {
              Map<String, ShardState> map = result.get(entry.getKey());
              if (map == null) {
                map = new HashMap<String, ShardState>();
                result.put(entry.getKey(), map);
              }
              map.putAll(entry.getValue());
            }
          }
          return result;
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get shard server layout [{0}]", e, table);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to get shard server layout [{0}]", e, table);
    }
  }

  @Override
  public long recordFrequency(final String table, final String columnFamily, final String columnName, final String value)
      throws BlurException, TException {
    try {
      checkTable(table);
      return scatterGather(getCluster(table), new BlurCommand<Long>() {
        @Override
        public Long call(Client client) throws BlurException, TException {
          return client.recordFrequency(table, columnFamily, columnName, value);
        }
      }, new Merger<Long>() {

        @Override
        public Long merge(BlurExecutorCompletionService<Long> service) throws BlurException {
          Long total = 0L;
          while (service.getRemainingCount() > 0) {
            Future<Long> future = service.poll(_defaultParallelCallTimeout, TimeUnit.MILLISECONDS, true, table,
                columnFamily, columnName, value);
            total += service.getResultThrowException(future, table, columnFamily, columnName, value);
          }
          return total;
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get record frequency [{0}/{1}/{2}/{3}]", e, table, columnFamily,
          columnName, value);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to get record frequency [{0}/{1}/{2}/{3}]", e, table,
          columnFamily, columnName, value);
    }
  }

  @Override
  public List<String> terms(final String table, final String columnFamily, final String columnName,
      final String startWith, final short size) throws BlurException, TException {
    try {
      checkTable(table);
      return scatterGather(getCluster(table), new BlurCommand<List<String>>() {
        @Override
        public List<String> call(Client client) throws BlurException, TException {
          return client.terms(table, columnFamily, columnName, startWith, size);
        }
      }, new Merger<List<String>>() {
        @Override
        public List<String> merge(BlurExecutorCompletionService<List<String>> service) throws BlurException {
          SortedSet<String> terms = new TreeSet<String>();
          while (service.getRemainingCount() > 0) {
            Future<List<String>> future = service.poll(_defaultParallelCallTimeout, TimeUnit.MILLISECONDS, true, table,
                columnFamily, columnName, startWith, size);
            terms.addAll(service.getResultThrowException(future, table, columnFamily, columnName, startWith, size));
          }
          return new ArrayList<String>(terms).subList(0, Math.min(terms.size(), size));
        }
      });
    } catch (Exception e) {
      LOG.error(
          "Unknown error while trying to terms table [{0}] columnFamily [{1}] columnName [{2}] startWith [{3}] size [{4}]",
          e, table, columnFamily, columnName, startWith, size);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(
          "Unknown error while trying to terms table [{0}] columnFamily [{1}] columnName [{2}] startWith [{3}] size [{4}]",
          e, table, columnFamily, columnName, startWith, size);
    }
  }

  private String getNode(String table, String rowId) throws BlurException, TException {
    Map<String, String> layout = shardServerLayout(table);
    int numberOfShards = getShardCount(table);
    String shardName = MutationHelper.getShardName(table, rowId, numberOfShards, _blurPartitioner);
    return layout.get(shardName);
  }

  private String getNode(String table, Selector selector) throws BlurException, TException {
    Map<String, String> layout = shardServerLayout(table);
    String locationId = selector.locationId;
    if (locationId != null) {
      String shard = locationId.substring(0, locationId.indexOf('/'));
      return layout.get(shard);
    }
    int numberOfShards = getShardCount(table);
    if (selector.rowId != null) {
      String shardName = MutationHelper.getShardName(table, selector.rowId, numberOfShards, _blurPartitioner);
      return layout.get(shardName);
    }
    throw new BException("Selector is missing both a locationid and a rowid, one is needed.");
  }

  private <R> R scatterGather(String cluster, final BlurCommand<R> command, Merger<R> merger) throws Exception {
    return ForkJoin.execute(_executor, _clusterStatus.getOnlineShardServers(true, cluster),
        new ParallelCall<String, R>() {
          @Override
          public R call(String hostnamePort) throws BlurException, TException, IOException {
            return _client.execute(hostnamePort, command.clone(), _maxDefaultRetries, _defaultDelay, _maxDefaultDelay);
          }
        }).merge(merger);
  }

  private <R> void scatter(String cluster, BlurCommand<R> command) throws Exception {
    scatterGather(cluster, command, new Merger<R>() {
      @Override
      public R merge(BlurExecutorCompletionService<R> service) throws BlurException {
        while (service.getRemainingCount() > 0) {
          Future<R> future = service.poll(_defaultParallelCallTimeout, TimeUnit.MILLISECONDS, true);
          service.getResultThrowException(future);
        }
        return null;
      }
    });
  }

  public static Schema merge(Schema result, Schema schema) {
    Map<String, Map<String, ColumnDefinition>> destColumnFamilies = result.getFamilies();
    Map<String, Map<String, ColumnDefinition>> srcColumnFamilies = schema.getFamilies();
    for (String srcColumnFamily : srcColumnFamilies.keySet()) {
      Map<String, ColumnDefinition> destColumnNames = destColumnFamilies.get(srcColumnFamily);
      Map<String, ColumnDefinition> srcColumnNames = srcColumnFamilies.get(srcColumnFamily);
      if (destColumnNames == null) {
        destColumnFamilies.put(srcColumnFamily, srcColumnNames);
      } else {
        destColumnNames.putAll(srcColumnNames);
      }
    }
    return result;
  }

  @Override
  public void mutate(final RowMutation mutation) throws BlurException, TException {
    try {
      checkTable(mutation.table);
      checkForUpdates(mutation.table);
      MutationHelper.validateMutation(mutation);
      String table = mutation.getTable();

      int numberOfShards = getShardCount(table);
      Map<String, String> tableLayout = getTableLayout(table);
      if (tableLayout.size() != numberOfShards) {
        throw new BException("Cannot update data while shard is missing");
      }

      String shardName = MutationHelper.getShardName(table, mutation.rowId, numberOfShards, _blurPartitioner);
      String node = tableLayout.get(shardName);
      _client.execute(node, new BlurCommand<Void>() {
        @Override
        public Void call(Client client) throws BlurException, TException {
          client.mutate(mutation);
          return null;
        }
      }, _maxMutateRetries, _mutateDelay, _maxMutateDelay);
    } catch (Exception e) {
      LOG.error("Unknown error during mutation of [{0}]", e, mutation);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error during mutation of [{0}]", e, mutation);
    }
  }

  @Override
  public void enqueueMutate(final RowMutation mutation) throws BlurException, TException {
    try {
      checkTable(mutation.table);
      checkForUpdates(mutation.table);
      MutationHelper.validateMutation(mutation);
      String table = mutation.getTable();

      int numberOfShards = getShardCount(table);
      Map<String, String> tableLayout = getTableLayout(table);
      if (tableLayout.size() != numberOfShards) {
        throw new BException("Cannot update data while shard is missing");
      }

      String shardName = MutationHelper.getShardName(table, mutation.rowId, numberOfShards, _blurPartitioner);
      String node = tableLayout.get(shardName);
      _client.execute(node, new BlurCommand<Void>() {
        @Override
        public Void call(Client client) throws BlurException, TException {
          client.enqueueMutate(mutation);
          return null;
        }
      }, _maxMutateRetries, _mutateDelay, _maxMutateDelay);
    } catch (Exception e) {
      LOG.error("Unknown error during enqueue mutation of [{0}]", e, mutation);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error during enqueue mutation of [{0}]", e, mutation);
    }
  }

  private Map<String, String> getTableLayout(String table) throws BlurException, TException {
    final long s = System.nanoTime();

    // wait for up to limit for value to not be null.
    while (true) {
      if (s + _tableLayoutTimeoutNanos < System.nanoTime()) {
        throw new BException("Could not get table [{0}] layout.", table);
      }
      Map<String, Map<String, String>> map = _shardServerLayout.get();
      Map<String, String> layout = map.get(table);
      if (layout != null) {
        return layout;
      } else {
        String cluster = getCluster(table);
        updateLayout(cluster);
      }
    }
  }

  private int getShardCount(String table) throws BlurException, TException {
    Integer numberOfShards = _tableShardCountMap.get(table);
    if (numberOfShards == null) {
      TableDescriptor descriptor = describe(table);
      numberOfShards = descriptor.shardCount;
      _tableShardCountMap.put(table, numberOfShards);
    }
    return numberOfShards;
  }

  @Override
  public void mutateBatch(List<RowMutation> mutations) throws BlurException, TException {
    try {
      for (RowMutation mutation : mutations) {
        MutationHelper.validateMutation(mutation);
      }
      Map<String, List<RowMutation>> batches = new HashMap<String, List<RowMutation>>();
      for (RowMutation mutation : mutations) {
        checkTable(mutation.table);
        checkForUpdates(mutation.table);

        MutationHelper.validateMutation(mutation);
        String table = mutation.getTable();

        int numberOfShards = getShardCount(table);
        Map<String, String> tableLayout = getTableLayout(table);
        if (tableLayout == null || tableLayout.size() != numberOfShards) {
          throw new BException("Cannot update data while shard is missing");
        }

        String shardName = MutationHelper.getShardName(table, mutation.rowId, numberOfShards, _blurPartitioner);
        String node = tableLayout.get(shardName);
        List<RowMutation> list = batches.get(node);
        if (list == null) {
          list = new ArrayList<RowMutation>();
          batches.put(node, list);
        }
        list.add(mutation);
      }

      List<Future<Void>> futures = new ArrayList<Future<Void>>();

      for (Entry<String, List<RowMutation>> entry : batches.entrySet()) {
        final String node = entry.getKey();
        final List<RowMutation> mutationsLst = entry.getValue();
        futures.add(_executor.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            return _client.execute(node, new BlurCommand<Void>() {
              @Override
              public Void call(Client client) throws BlurException, TException {
                client.mutateBatch(mutationsLst);
                return null;
              }
            }, _maxMutateRetries, _mutateDelay, _maxMutateDelay);
          }
        }));
      }

      for (Future<Void> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          LOG.error("Unknown error during batch mutations", e);
          throw new BException("Unknown error during batch mutations", e);
        } catch (ExecutionException e) {
          LOG.error("Unknown error during batch mutations", e.getCause());
          throw new BException("Unknown error during batch mutations", e.getCause());
        }
      }
    } catch (Exception e) {
      LOG.error("Unknown error during batch mutation.", e);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error during mutation.", e);
    }
  }

  @Override
  public void enqueueMutateBatch(List<RowMutation> mutations) throws BlurException, TException {
    try {
      for (RowMutation mutation : mutations) {
        MutationHelper.validateMutation(mutation);
      }
      Map<String, List<RowMutation>> batches = new HashMap<String, List<RowMutation>>();
      for (RowMutation mutation : mutations) {
        checkTable(mutation.table);
        checkForUpdates(mutation.table);

        MutationHelper.validateMutation(mutation);
        String table = mutation.getTable();

        int numberOfShards = getShardCount(table);
        Map<String, String> tableLayout = getTableLayout(table);
        if (tableLayout == null || tableLayout.size() != numberOfShards) {
          throw new BException("Cannot update data while shard is missing");
        }

        String shardName = MutationHelper.getShardName(table, mutation.rowId, numberOfShards, _blurPartitioner);
        String node = tableLayout.get(shardName);
        List<RowMutation> list = batches.get(node);
        if (list == null) {
          list = new ArrayList<RowMutation>();
          batches.put(node, list);
        }
        list.add(mutation);
      }

      List<Future<Void>> futures = new ArrayList<Future<Void>>();

      for (Entry<String, List<RowMutation>> entry : batches.entrySet()) {
        final String node = entry.getKey();
        final List<RowMutation> mutationsLst = entry.getValue();
        futures.add(_executor.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            return _client.execute(node, new BlurCommand<Void>() {
              @Override
              public Void call(Client client) throws BlurException, TException {
                client.enqueueMutateBatch(mutationsLst);
                return null;
              }
            }, _maxMutateRetries, _mutateDelay, _maxMutateDelay);
          }
        }));
      }

      for (Future<Void> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          LOG.error("Unknown error during batch enqueue mutations", e);
          throw new BException("Unknown error during batch enqueue mutations", e);
        } catch (ExecutionException e) {
          LOG.error("Unknown error during batch enqueue mutations", e.getCause());
          throw new BException("Unknown error during batch enqueue mutations", e.getCause());
        }
      }
    } catch (Exception e) {
      LOG.error("Unknown error during batch mutation.", e);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error during mutation.", e);
    }
  }

  @Override
  public void createSnapshot(final String table, final String name) throws BlurException, TException {
    try {
      checkTable(table);
      scatter(getCluster(table), new BlurCommand<Void>() {
        @Override
        public Void call(Client client) throws BlurException, TException {
          client.createSnapshot(table, name);
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to create a snapshot [{0}] of table [{1}]", e, name, table);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to create a snapshot [{0}] of table [{1}]", e, name, table);
    }
  }

  @Override
  public void removeSnapshot(final String table, final String name) throws BlurException, TException {
    try {
      checkTable(table);
      scatter(getCluster(table), new BlurCommand<Void>() {
        @Override
        public Void call(Client client) throws BlurException, TException {
          client.removeSnapshot(table, name);
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to remove a snapshot [{0}] of table [{1}]", e, name, table);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to remove a snapshot [{0}] of table [{1}]", e, name, table);
    }
  }

  @Override
  public Map<String, List<String>> listSnapshots(final String table) throws BlurException, TException {
    try {
      checkTable(table);
      return scatterGather(getCluster(table), new BlurCommand<Map<String, List<String>>>() {
        @Override
        public Map<String, List<String>> call(Client client) throws BlurException, TException {
          return client.listSnapshots(table);
        }
      }, new Merger<Map<String, List<String>>>() {
        @Override
        public Map<String, List<String>> merge(BlurExecutorCompletionService<Map<String, List<String>>> service)
            throws BlurException {
          Map<String, List<String>> result = new HashMap<String, List<String>>();
          while (service.getRemainingCount() > 0) {
            Future<Map<String, List<String>>> future = service.poll(_defaultParallelCallTimeout, TimeUnit.MILLISECONDS,
                true);
            Map<String, List<String>> snapshotsOnAShardServer = service.getResultThrowException(future);
            for (Entry<String, List<String>> entry : snapshotsOnAShardServer.entrySet()) {
              List<String> snapshots = result.get(entry.getKey());
              if (snapshots == null) {
                snapshots = new ArrayList<String>();
                result.put(entry.getKey(), snapshots);
              }
              snapshots.addAll(entry.getValue());
            }
          }
          return result;
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get the list of snapshots for table [{0}]", e, table);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to get the list of snapshots for table [{0}]", e, table);
    }
  }

  public int getRemoteFetchCount() {
    return _remoteFetchCount;
  }

  public void setRemoteFetchCount(int remoteFetchCount) {
    _remoteFetchCount = remoteFetchCount;
  }

  public void setQueryChecker(BlurQueryChecker queryChecker) {
    _queryChecker = queryChecker;
  }

  public void setThreadCount(int threadCount) {
    _threadCount = threadCount;
  }

  public void setMaxFetchRetries(int maxFetchRetries) {
    _maxFetchRetries = maxFetchRetries;
  }

  public void setMaxMutateRetries(int maxMutateRetries) {
    _maxMutateRetries = maxMutateRetries;
  }

  public void setMaxDefaultRetries(int maxDefaultRetries) {
    _maxDefaultRetries = maxDefaultRetries;
  }

  public void setFetchDelay(long fetchDelay) {
    _fetchDelay = fetchDelay;
  }

  public void setMutateDelay(long mutateDelay) {
    _mutateDelay = mutateDelay;
  }

  public void setDefaultDelay(long defaultDelay) {
    _defaultDelay = defaultDelay;
  }

  public void setMaxFetchDelay(long maxFetchDelay) {
    _maxFetchDelay = maxFetchDelay;
  }

  public void setMaxMutateDelay(long maxMutateDelay) {
    _maxMutateDelay = maxMutateDelay;
  }

  public void setMaxDefaultDelay(long maxDefaultDelay) {
    _maxDefaultDelay = maxDefaultDelay;
  }

  public BlurClient getClient() {
    return _client;
  }

  public void setClient(BlurClient client) {
    _client = client;
  }

  @Override
  public void optimize(final String table, final int numberOfSegmentsPerShard) throws BlurException, TException {
    checkTable(table);
    try {
      checkTable(table);
      scatter(getCluster(table), new BlurCommand<Void>() {
        @Override
        public Void call(Client client) throws BlurException, TException {
          client.optimize(table, numberOfSegmentsPerShard);
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to optimize [table={0},numberOfSegmentsPerShard={1}]", e, table,
          numberOfSegmentsPerShard);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to optimize [table={0},numberOfSegmentsPerShard={1}]", e, table,
          numberOfSegmentsPerShard);
    }
  }

  @Override
  public String parseQuery(final String table, final Query simpleQuery) throws BlurException, TException {
    try {
      checkTable(table);
      String cluster = getCluster(table);
      List<String> onlineShardServers = _clusterStatus.getOnlineShardServers(true, cluster);
      return BlurClientManager.execute(getConnections(onlineShardServers), new BlurCommand<String>() {
        @Override
        public String call(Client client) throws BlurException, TException {
          return client.parseQuery(table, simpleQuery);
        }
      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to parse query [table={0},simpleQuery={1}]", e, table, simpleQuery);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to parse query [table={0},simpleQuery={1}]", e, table,
          simpleQuery);
    }
  }

  private List<Connection> getConnections(List<String> onlineShardServers) {
    List<Connection> connections = new ArrayList<Connection>();
    for (String c : onlineShardServers) {
      connections.add(new Connection(c));
    }
    return connections;
  }

  @Override
  public void setUser(User user) throws TException {
    ControllerServerContext context = ControllerServerContext.getControllerServerContext();
    context.setUser(user);
  }

  @Override
  public void startTrace(String rootId, String requestId) throws TException {
    ControllerServerContext context = ControllerServerContext.getControllerServerContext();
    context.setTraceRootId(rootId);
    context.setTraceRequestId(requestId);
  }

  @Override
  public org.apache.blur.thrift.generated.Response execute(String commandName, Arguments arguments)
      throws BlurException, TException {
    try {
      BlurObject args = CommandUtil.toBlurObject(arguments);
      CommandStatus originalCommandStatusObject = new CommandStatus(null, commandName, arguments, null, null);
      Response response = _commandManager.execute(getTableContextFactory(), getLayoutFactory(), commandName,
          new ArgumentOverlay(args, _serDe), originalCommandStatusObject);
      return CommandUtil.fromObjectToThrift(response, _serDe);
    } catch (Exception e) {
      if (e instanceof org.apache.blur.command.TimeoutException) {
        throw new TimeoutException(((org.apache.blur.command.TimeoutException) e).getInstanceExecutionId());
      }
      LOG.error("Unknown error while trying to execute command [{0}]", e, commandName);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  private TableContextFactory getTableContextFactory() {
    return new TableContextFactory() {
      @Override
      public TableContext getTableContext(String table) throws IOException {
        return TableContext.create(_clusterStatus.getTableDescriptor(true, _clusterStatus.getCluster(true, table),
            table));
      }
    };
  }

  private LayoutFactory getLayoutFactory() {
    return new LayoutFactory() {

      @Override
      public Set<Shard> getServerLayout(Server server) throws IOException {
        makeSureLayoutIsUpToDate();
        Set<Shard> shardSet = new TreeSet<Shard>();
        Map<String, Map<String, String>> map = _shardServerLayout.get();
        for (Entry<String, Map<String, String>> e1 : map.entrySet()) {
          String table = e1.getKey();
          Map<String, String> layout = e1.getValue();
          for (Entry<String, String> e2 : layout.entrySet()) {
            String shardId = e2.getKey();
            String serverId = e2.getValue();
            if (serverId.equals(server.getServer())) {
              shardSet.add(new Shard(table, shardId));
            }
          }
        }
        return shardSet;
      }

      private void makeSureLayoutIsUpToDate() throws IOException {
        List<String> tableList = _clusterStatus.getTableList(true);
        for (String table : tableList) {
          try {
            getTableLayout(table);
          } catch (BlurException e) {
            throw new IOException(e);
          } catch (TException e) {
            throw new IOException(e);
          }
        }
      }

      @Override
      public Set<Connection> getServerConnections() {
        List<String> clusterList = _clusterStatus.getClusterList(true);
        Set<Connection> connections = new HashSet<Connection>();
        for (String cluster : clusterList) {
          List<String> onlineShardServers = _clusterStatus.getOnlineShardServers(true, cluster);
          for (String server : onlineShardServers) {
            connections.add(new Connection(server));
          }
        }
        return connections;
      }

      @Override
      public boolean isValidServer(Server server, Set<String> tables, Set<Shard> shards) {
        for (String table : tables) {
          String cluster = _clusterStatus.getCluster(true, table);
          List<String> onlineShardServers = _clusterStatus.getOnlineShardServers(true, cluster);
          if (!onlineShardServers.contains(server.getServer())) {
            return false;
          }
          if (shards == null || shards.isEmpty()) {
            return true;
          }
          Map<String, Map<String, String>> layout = _shardServerLayout.get();
          if (layout == null) {
            return false;
          }
          Map<String, String> shardIdToServerMap = layout.get(table);
          if (shardIdToServerMap == null) {
            return false;
          }
          for (Shard shard : shards) {
            String serverId = shardIdToServerMap.get(shard.getShard());
            if (serverId.equals(server.getServer())) {
              return true;
            }
          }
        }
        return false;
      }
    };
  }

  public void setCommandManager(ControllerCommandManager commandManager) {
    _commandManager = commandManager;
  }

  @Override
  public org.apache.blur.thrift.generated.Response reconnect(long instanceExecutionId) throws BlurException,
      TimeoutException, TException {
    try {
      Response response = _commandManager.reconnect(instanceExecutionId);
      return CommandUtil.fromObjectToThrift(response, _serDe);
    } catch (Exception e) {
      if (e instanceof org.apache.blur.command.TimeoutException) {
        throw new TimeoutException(((org.apache.blur.command.TimeoutException) e).getInstanceExecutionId());
      }
      LOG.error("Unknown error while trying to reconnect to executing command [{0}]", e, instanceExecutionId);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public List<CommandDescriptor> listInstalledCommands() throws BlurException, TException {
    try {
      return listInstalledCommands(_commandManager);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get a list of installed commands [{0}]", e);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void refresh() throws TException {
    // This is a NO-OP at this point for the controller.
  }

  @Override
  public void bulkMutateAdd(final String bulkId, final RowMutation mutation) throws BlurException, TException {
    try {
      String table = mutation.getTable();
      checkTable(table);
      checkForUpdates(table);
      MutationHelper.validateMutation(mutation);

      int numberOfShards = getShardCount(table);
      Map<String, String> tableLayout = getTableLayout(table);
      if (tableLayout.size() != numberOfShards) {
        throw new BException("Cannot update data while shard is missing");
      }
      String shardName = MutationHelper.getShardName(table, mutation.rowId, numberOfShards, _blurPartitioner);
      String node = tableLayout.get(shardName);
      _client.execute(node, new BlurCommand<Void>() {
        @Override
        public Void call(Client client) throws BlurException, TException {
          client.bulkMutateAdd(bulkId, mutation);
          return null;
        }
      }, _maxMutateRetries, _mutateDelay, _maxMutateDelay);
    } catch (Exception e) {
      LOG.error("Unknown error during bulk mutation of [{0}]", e, bulkId);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error during bulk mutation of [{0}]", e, bulkId);
    }
  }

  @Override
  public void bulkMutateAddMultiple(final String bulkId, List<RowMutation> rowMutations) throws BlurException,
      TException {
    try {
      Map<String, List<RowMutation>> batches = batchByServer(rowMutations);
      for (Entry<String, List<RowMutation>> entry : batches.entrySet()) {
        String node = entry.getKey();
        final List<RowMutation> batch = entry.getValue();
        _client.execute(node, new BlurCommand<Void>() {
          @Override
          public Void call(Client client) throws BlurException, TException {
            client.bulkMutateAddMultiple(bulkId, batch);
            return null;
          }
        }, _maxMutateRetries, _mutateDelay, _maxMutateDelay);
      }
    } catch (Exception e) {
      LOG.error("Unknown error during bulk mutation of [{0}]", e, bulkId);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error during bulk mutation of [{0}]", e, bulkId);
    }
  }

  private Map<String, List<RowMutation>> batchByServer(List<RowMutation> rowMutations) throws TException {
    Map<String, List<RowMutation>> result = new HashMap<String, List<RowMutation>>();
    for (RowMutation rowMutation : rowMutations) {
      String table = rowMutation.getTable();
      checkTable(table);
      checkForUpdates(table);
      int numberOfShards = getShardCount(table);
      Map<String, String> tableLayout = getTableLayout(table);
      if (tableLayout.size() != numberOfShards) {
        throw new BException("Cannot update data while shard is missing");
      }
      String shardName = MutationHelper.getShardName(table, rowMutation.getRowId(), numberOfShards, _blurPartitioner);
      String node = tableLayout.get(shardName);

      List<RowMutation> list = result.get(node);
      if (list == null) {
        result.put(node, list = new ArrayList<RowMutation>());
      }
      MutationHelper.validateMutation(rowMutation);
      list.add(rowMutation);
    }
    return result;
  }

  @Override
  public void bulkMutateFinish(final String bulkId, final boolean apply, final boolean blockUntilComplete)
      throws BlurException, TException {
    List<String> shardClusterList = shardClusterList();
    for (String cluster : shardClusterList) {
      try {
        scatter(cluster, new BlurCommand<Void>() {
          @Override
          public Void call(Client client) throws BlurException, TException {
            client.bulkMutateFinish(bulkId, apply, blockUntilComplete);
            return null;
          }
        });
      } catch (Exception e) {
        LOG.error("Unknown error while trying to get finish a bulk mutate [{0}] [{1}]", e, bulkId);
        if (e instanceof BlurException) {
          throw (BlurException) e;
        }
        throw new BException(e.getMessage(), e);
      }
    }
  }

  @Override
  public void loadData(String table, String location) throws BlurException, TException {
    try {
      String cluster = getCluster(table);
      TableDescriptor tableDescriptor = _clusterStatus.getTableDescriptor(true, cluster, table);
      TableContext tableContext = TableContext.create(tableDescriptor);
      Configuration configuration = tableContext.getConfiguration();
      Path path = new Path(location);
      FileSystem fileSystem = path.getFileSystem(configuration);
      validateOrLoadIndexes(cluster, table, path, fileSystem, tableDescriptor, configuration, false);
      validateOrLoadIndexes(cluster, table, path, fileSystem, tableDescriptor, configuration, true);
    } catch (IOException e) {
      throw new BException(e.getMessage(), e);
    }
  }

  private void validateOrLoadIndexes(String cluster, final String table, Path path, FileSystem fileSystem,
      TableDescriptor descriptor, Configuration configuration, final boolean load) throws IOException, BlurException,
      TException {
    int shardCount = descriptor.getShardCount();
    FileStatus[] listStatus = fileSystem.listStatus(path);
    int count = 0;
    final Map<String, List<String>> shardServerPathMap = new HashMap<String, List<String>>();
    Map<String, String> shardServerLayout = shardServerLayout(descriptor.getName());
    for (FileStatus fileStatus : listStatus) {
      Path shardPath = fileStatus.getPath();
      String shardId = shardPath.getName();
      String server = shardServerLayout.get(shardId);

      int shardIndex = ShardUtil.getShardIndex(shardId);
      if (shardIndex >= shardCount) {
        throw new IOException("Too many shards [" + shardIndex + "].");
      }
      List<String> paths = shardServerPathMap.get(server);
      if (paths == null) {
        shardServerPathMap.put(server, paths = new ArrayList<String>());
      }
      paths.add(shardPath.toString());
      count++;
    }
    if (shardCount != count) {
      throw new IOException("Not enough shards [" + count + "] should be [" + shardCount + "].");
    }
    try {
      scatter(cluster, new BlurCommand<Void>() {
        @Override
        public Void call(Client client) throws BlurException, TException {
          throw new RuntimeException("Not Used.");
        }

        @Override
        public Void call(Client client, Connection connection) throws BlurException, TException {
          String server = connection.getHost() + ":" + connection.getPort();
          List<String> externalIndexPaths = shardServerPathMap.get(server);
          if (externalIndexPaths == null || externalIndexPaths.isEmpty()) {
            return null;
          }
          if (load) {
            client.loadIndex(table, externalIndexPaths);
          } else {
            client.validateIndex(table, externalIndexPaths);
          }
          return null;
        }

      });
    } catch (Exception e) {
      LOG.error("Unknown error while trying to validate indexes for table [{0}]", e, table);
      if (e instanceof BlurException) {
        throw (BlurException) e;
      }
      throw new BException("Unknown error while trying to validate indexes for table [{0}]", e, table);
    }
  }

  @Override
  public List<String> commandStatusList(int startingAt, short fetch) throws BlurException, TException {
    try {
      List<String> shardClusterList = shardClusterList();
      SortedSet<String> result = new TreeSet<String>();
      result.addAll(_commandManager.commandStatusList());
      for (String cluster : shardClusterList) {
        result.addAll(scatterGather(cluster, new BlurCommand<List<String>>() {
          @Override
          public List<String> call(Client client) throws BlurException, TException {
            return client.commandStatusList(0, Short.MAX_VALUE);
          }
        }, new Merger<List<String>>() {
          @Override
          public List<String> merge(BlurExecutorCompletionService<List<String>> service) throws BlurException {
            SortedSet<String> ids = new TreeSet<String>();
            while (service.getRemainingCount() > 0) {
              Future<List<String>> future = service.poll(_defaultParallelCallTimeout, TimeUnit.MILLISECONDS, true);
              ids.addAll(service.getResultThrowException(future));
            }
            return new ArrayList<String>(ids);
          }
        }));
      }
      return new ArrayList<String>(result).subList(startingAt, Math.min(fetch, result.size()));
    } catch (Exception e) {
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public CommandStatus commandStatus(String commandExecutionId) throws BlurException, TException {
    try {
      List<String> shardClusterList = shardClusterList();
      CommandStatus commandStatus = _commandManager.getCommandStatus(commandExecutionId);
      for (String cluster : shardClusterList) {
        CommandStatus cs = scatterGather(cluster, new BlurCommand<CommandStatus>() {
          @Override
          public CommandStatus call(Client client) throws BlurException, TException {
            return client.commandStatus(commandExecutionId);
          }
        }, new Merger<CommandStatus>() {
          @Override
          public CommandStatus merge(BlurExecutorCompletionService<CommandStatus> service) throws BlurException {
            CommandStatus commandStatus = null;
            while (service.getRemainingCount() > 0) {
              Future<CommandStatus> future = service.poll(_defaultParallelCallTimeout, TimeUnit.MILLISECONDS, true);
              commandStatus = mergeCommandStatus(commandStatus, service.getResultThrowException(future));
            }
            return commandStatus;
          }
        });
        commandStatus = mergeCommandStatus(commandStatus, cs);
      }
      return commandStatus;
    } catch (Exception e) {
      throw new BException(e.getMessage(), e);
    }
  }

  private static CommandStatus mergeCommandStatus(CommandStatus cs1, CommandStatus cs2) {
    if (cs1 == null && cs2 == null) {
      return null;
    } else if (cs1 == null) {
      return cs2;
    } else if (cs2 == null) {
      return cs1;
    } else {
      Map<String, Map<CommandStatusState, Long>> serverStateMap1 = cs1.getServerStateMap();
      Map<String, Map<CommandStatusState, Long>> serverStateMap2 = cs2.getServerStateMap();
      Map<String, Map<CommandStatusState, Long>> merge = mergeServerStateMap(serverStateMap1, serverStateMap2);
      return new CommandStatus(cs1.getExecutionId(), cs1.getCommandName(), cs1.getArguments(), merge, cs1.getUser());
    }
  }

  private static Map<String, Map<CommandStatusState, Long>> mergeServerStateMap(
      Map<String, Map<CommandStatusState, Long>> serverStateMap1,
      Map<String, Map<CommandStatusState, Long>> serverStateMap2) {
    Map<String, Map<CommandStatusState, Long>> result = new HashMap<String, Map<CommandStatusState, Long>>();
    Set<String> keys = new HashSet<String>();
    keys.addAll(serverStateMap1.keySet());
    keys.addAll(serverStateMap2.keySet());
    for (String key : keys) {
      Map<CommandStatusState, Long> css1 = serverStateMap2.get(key);
      Map<CommandStatusState, Long> css2 = serverStateMap2.get(key);
      result.put(key, mergeCommandStatusState(css1, css2));
    }
    return result;
  }

  private static Map<CommandStatusState, Long> mergeCommandStatusState(Map<CommandStatusState, Long> css1,
      Map<CommandStatusState, Long> css2) {
    if (css1 == null && css2 == null) {
      return new HashMap<CommandStatusState, Long>();
    } else if (css1 == null) {
      return css2;
    } else if (css2 == null) {
      return css1;
    } else {
      Map<CommandStatusState, Long> result = new HashMap<CommandStatusState, Long>(css1);
      for (Entry<CommandStatusState, Long> e : css2.entrySet()) {
        CommandStatusState key = e.getKey();
        Long l = result.get(key);
        Long value = e.getValue();
        if (l == null) {
          result.put(key, value);
        } else {
          result.put(key, l + value);
        }
      }
      return result;
    }
  }

  @Override
  public void commandCancel(String commandExecutionId) throws BlurException, TException {
    try {
      List<String> shardClusterList = shardClusterList();
      _commandManager.cancelCommand(commandExecutionId);
      for (String cluster : shardClusterList) {
        scatter(cluster, new BlurCommand<Void>() {
          @Override
          public Void call(Client client) throws BlurException, TException {
            client.commandCancel(commandExecutionId);
            return null;
          }
        });
      }
    } catch (Exception e) {
      throw new BException(e.getMessage(), e);
    }
  }
}
