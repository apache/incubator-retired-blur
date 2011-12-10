package com.nearinfinity.blur.manager.indexserver;

import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC;
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC_VALUE;
import static com.nearinfinity.blur.utils.BlurConstants.SHARD_PREFIX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.search.Similarity;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.index.DirectIODirectory;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.manager.BlurFilterCache;
import com.nearinfinity.blur.manager.clusterstatus.ClusterStatus;
import com.nearinfinity.blur.manager.clusterstatus.ZookeeperPathConstants;
import com.nearinfinity.blur.manager.indexserver.utils.CreateTable;
import com.nearinfinity.blur.manager.writer.BlurIndex;
import com.nearinfinity.blur.manager.writer.BlurIndexCloser;
import com.nearinfinity.blur.manager.writer.BlurIndexCommiter;
import com.nearinfinity.blur.manager.writer.BlurIndexRefresher;
import com.nearinfinity.blur.manager.writer.BlurIndexWriter;
import com.nearinfinity.blur.metrics.BlurMetrics;
import com.nearinfinity.blur.store.blockcache.BlockDirectory;
import com.nearinfinity.blur.store.blockcache.BlockDirectoryCache;
import com.nearinfinity.blur.store.compressed.CompressedFieldDataDirectory;
import com.nearinfinity.blur.store.hdfs.HdfsDirectory;
import com.nearinfinity.blur.store.lock.ZookeeperLockFactory;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurUtil;

public class DistributedIndexServer extends AbstractIndexServer {

  private static final Log LOG = LogFactory.getLog(DistributedIndexServer.class);
  private static final long _delay = TimeUnit.SECONDS.toMillis(5);

  private Map<String, BlurAnalyzer> _tableAnalyzers = new ConcurrentHashMap<String, BlurAnalyzer>();
  private Map<String, TableDescriptor> _tableDescriptors = new ConcurrentHashMap<String, TableDescriptor>();
  private Map<String, Similarity> _tableSimilarity = new ConcurrentHashMap<String, Similarity>();
  private Map<String, DistributedLayoutManager> _layoutManagers = new ConcurrentHashMap<String, DistributedLayoutManager>();
  private Map<String, Set<String>> _layoutCache = new ConcurrentHashMap<String, Set<String>>();
  private ConcurrentHashMap<String, Map<String, BlurIndex>> _indexes = new ConcurrentHashMap<String, Map<String, BlurIndex>>();

  // set externally
  private ClusterStatus _clusterStatus;
  private Configuration _configuration;
  private String _nodeName;
  private int _shardOpenerThreadCount;
  private BlurIndexRefresher _refresher;
  private BlurIndexCommiter _commiter;
  private BlockDirectoryCache _cache;
  private BlurMetrics _blurMetrics;
  private ZooKeeper _zookeeper;

  // set internally
  private Timer _timerCacheFlush;
  private ExecutorService _openerService;
  private BlurIndexCloser _closer;
  private Timer _timerTableWarmer;
  private BlurFilterCache _filterCache;
  private Thread _shardServerWatcherDaemon;
  private AtomicBoolean _running = new AtomicBoolean();
  private long _safeModeDelay;
  private BlurIndexWarmup _warmup = new DefaultBlurIndexWarmup();
  private IndexDeletionPolicy _indexDeletionPolicy;
  private String cluster = BlurConstants.BLUR_CLUSTER;
  
  public static interface ReleaseReader {
    void release() throws IOException;
  }

  public void init() throws KeeperException, InterruptedException {
    setupZookeeper();
    _openerService = Executors.newThreadPool("shard-opener", _shardOpenerThreadCount);
    _closer = new BlurIndexCloser();
    setupFlushCacheTimer();
    String lockPath = BlurUtil.lockForSafeMode(_zookeeper,getNodeName(),cluster);
    try {
      registerMyself();
      setupSafeMode();
    } finally {
      BlurUtil.unlockForSafeMode(_zookeeper,lockPath);
    }
    waitInSafeModeIfNeeded();
    _running.set(true);
    setupTableWarmer();
    watchForShardServerChanges();
  }
  
  private void setupZookeeper() throws KeeperException, InterruptedException {
    BlurUtil.createIfMissing(_zookeeper,ZookeeperPathConstants.getBasePath());
    BlurUtil.createIfMissing(_zookeeper,ZookeeperPathConstants.getOnlineControllersPath());
    BlurUtil.createIfMissing(_zookeeper,ZookeeperPathConstants.getClustersPath());
    BlurUtil.createIfMissing(_zookeeper,ZookeeperPathConstants.getClusterPath(cluster));
    BlurUtil.createIfMissing(_zookeeper,ZookeeperPathConstants.getSafemodePath(cluster));
    BlurUtil.createIfMissing(_zookeeper,ZookeeperPathConstants.getRegisteredShardsPath(cluster));
    BlurUtil.createIfMissing(_zookeeper,ZookeeperPathConstants.getOnlinePath(cluster));
    BlurUtil.createIfMissing(_zookeeper,ZookeeperPathConstants.getOnlineShardsPath(cluster));
    BlurUtil.createIfMissing(_zookeeper,ZookeeperPathConstants.getTablesPath(cluster));
  }

  private void watchForShardServerChanges() {
    _shardServerWatcherDaemon = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          synchronized (_layoutManagers) {
            try {
              _zookeeper.getChildren(ZookeeperPathConstants.getOnlineShardsPath(cluster), new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                  synchronized (_layoutManagers) {
                    _layoutManagers.notifyAll();
                  }
                }
              });
              _layoutManagers.clear();
              _layoutCache.clear();
              _layoutManagers.wait();
            } catch (KeeperException e) {
              if (e.code() == Code.SESSIONEXPIRED) {
                LOG.error("Zookeeper session expired.");
                _running.set(false);
                return;
              }
              LOG.error("Unknown Error",e);
            } catch (InterruptedException e) {
              LOG.error("Unknown Error",e);
            }
          }
        }
      }
    });
    _shardServerWatcherDaemon.setName("Shard-Server-Watcher-Daemon");
    _shardServerWatcherDaemon.setDaemon(true);
    _shardServerWatcherDaemon.start();
  }

  private void waitInSafeModeIfNeeded() throws KeeperException, InterruptedException {
    String blurSafemodePath = ZookeeperPathConstants.getSafemodePath(cluster);
    Stat stat = _zookeeper.exists(blurSafemodePath, false);
    if (stat == null) {
      throw new RuntimeException("Safemode path missing [" + blurSafemodePath + "]");
    }
    byte[] data = _zookeeper.getData(blurSafemodePath, false, stat);
    if (data == null) {
      throw new RuntimeException("Safemode data missing [" + blurSafemodePath + "]");
    }
    long timestamp = Long.parseLong(new String(data));
    long waitTime = timestamp - System.currentTimeMillis();
    if (waitTime > 0) {
      LOG.info("Waiting in safe mode for [{0}] seconds",waitTime/1000.0);
      Thread.sleep(waitTime);
    }
  }

  private void setupSafeMode() throws KeeperException, InterruptedException {
    String shardsPath = ZookeeperPathConstants.getOnlineShardsPath(cluster);
    List<String> children = _zookeeper.getChildren(shardsPath, false);
    if (children.size() == 0) {
      throw new RuntimeException("No shards registered!");
    }
    if (children.size() != 1) {
      return;
    }
    LOG.info("First node online, setting up safe mode.");
    long timestamp = System.currentTimeMillis() + _safeModeDelay;
    String blurSafemodePath = ZookeeperPathConstants.getSafemodePath(cluster);
    Stat stat = _zookeeper.exists(blurSafemodePath, false);
    if (stat == null) {
      _zookeeper.create(blurSafemodePath, Long.toString(timestamp).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } else {
      _zookeeper.setData(blurSafemodePath, Long.toString(timestamp).getBytes(), -1);
    }
    setupAnyMissingPaths();
    removeAnyTableLocks();
  }

  private void setupAnyMissingPaths() throws KeeperException, InterruptedException {
    String tablesPath = ZookeeperPathConstants.getTablesPath(cluster);
    List<String> tables = _zookeeper.getChildren(tablesPath, false);
    for (String table : tables) {
      createIfMissing(ZookeeperPathConstants.getLockPath(cluster, table));
      createIfMissing(ZookeeperPathConstants.getTableFieldNamesPath(cluster, table));
    }
  }

  private void createIfMissing(String path) throws KeeperException, InterruptedException {
    if (_zookeeper.exists(path, false) == null) {
      LOG.info("Creating missing path in zookeeper [{0}]",path);
      _zookeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  private void removeAnyTableLocks() {
    List<String> tableList = _clusterStatus.getTableList();
    for (String table : tableList) {
      _clusterStatus.clearLocks(table);
    }
  }

  private void setupTableWarmer() {
    _timerTableWarmer = new Timer("Table-Warmer", true);
    _timerTableWarmer.schedule(new TimerTask() {
      @Override
      public void run() {
        List<String> tableList = _clusterStatus.getTableList();
        for (String table : tableList) {
          try {
            int count = getIndexes(table).size();
            LOG.debug("Table [{0}] has [{1}] number of shards online in this node.",table,count);
          } catch (IOException e) {
            LOG.error("Unknown error trying to warm table [{0}]",e,table);
          }
        }
      }
    }, _delay, _delay);
  }

  private void registerMyself() {
    String nodeName = getNodeName();
    String registeredShardsPath = ZookeeperPathConstants.getRegisteredShardsPath(cluster) + "/" + nodeName;
    String onlineShardsPath = ZookeeperPathConstants.getOnlineShardsPath(cluster) + "/" + nodeName;
    try {
      if (_zookeeper.exists(registeredShardsPath, false) == null) {
        _zookeeper.create(registeredShardsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      while (_zookeeper.exists(onlineShardsPath, false) != null) {
        LOG.info("Node [{0}] already registered, waiting for path [{1}] to be released", nodeName, onlineShardsPath);
        Thread.sleep(3000);
      }
      String version = BlurUtil.getVersion();
      _zookeeper.create(onlineShardsPath, version.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void setupFlushCacheTimer() {
    _timerCacheFlush = new Timer("Flush-IndexServer-Caches", true);
    _timerCacheFlush.schedule(new TimerTask() {
      @Override
      public void run() {
        clearMapOfOldTables(_tableAnalyzers);
        clearMapOfOldTables(_tableDescriptors);
        clearMapOfOldTables(_layoutManagers);
        clearMapOfOldTables(_layoutCache);
        clearMapOfOldTables(_tableSimilarity);
        Map<String, Map<String, BlurIndex>> oldIndexesThatNeedToBeClosed = clearMapOfOldTables(_indexes);
        for (String table : oldIndexesThatNeedToBeClosed.keySet()) {
          Map<String, BlurIndex> indexes = oldIndexesThatNeedToBeClosed.get(table);
          if (indexes == null) {
            continue;
          }
          for (String shard : indexes.keySet()) {
            BlurIndex index = indexes.get(shard);
            if (index == null) {
              continue;
            }
            close(index,table,shard);
          }
        }
        for (String table : _indexes.keySet()) {
          Map<String, BlurIndex> shardMap = _indexes.get(table);
          if (shardMap != null) {
            Set<String> shards = new HashSet<String>(shardMap.keySet());
            Set<String> shardsToServe = getShardsToServe(table);
            shards.removeAll(shardsToServe);
            if (!shards.isEmpty()) {
              LOG.info("Need to close indexes for table [{0}] indexes [{1}]",table,shards);
            }
            for (String shard : shards) {
              LOG.info("Closing index for table [{0}] shard [{1}]",table,shard);
              BlurIndex index = shardMap.remove(shard);
              close(index, table, shard);
            }
          }
        }
        
      }
    }, _delay, _delay);
  }

  protected void close(BlurIndex index, String table, String shard) {
    LOG.info("Closing index [{0}] from table [{1}] shard [{2}]", index, table, shard);
    try {
      _filterCache.closing(table,shard,index);
      index.close();
    } catch (IOException e) {
      LOG.error("Error while closing index [{0}] from table [{1}] shard [{2}]", e, index, table, shard);
    }
  }

  protected <T> Map<String, T> clearMapOfOldTables(Map<String, T> map) {
    List<String> tables = new ArrayList<String>(map.keySet());
    Map<String, T> removed = new HashMap<String, T>();
    for (String table : tables) {
      if (!_clusterStatus.exists(true,table)) {
        removed.put(table, map.remove(table));
      }
    }
    for (String table : tables) {
      if (!_clusterStatus.isEnabled(true,table)) {
        removed.put(table, map.remove(table));
      }
    }
    return removed;
  }

  @Override
  public void close() {
    _running.set(false);
    _timerCacheFlush.purge();
    _timerCacheFlush.cancel();
  }

  @Override
  public BlurAnalyzer getAnalyzer(String table) {
    checkTable(table);
    BlurAnalyzer blurAnalyzer = _tableAnalyzers.get(table);
    if (blurAnalyzer == null) {
      TableDescriptor descriptor = getTableDescriptor(table);
      blurAnalyzer = new BlurAnalyzer(descriptor.analyzerDefinition);
      _tableAnalyzers.put(table, blurAnalyzer);
    }
    return blurAnalyzer;
  }

  @Override
  public int getCompressionBlockSize(String table) {
    checkTable(table);
    TableDescriptor descriptor = getTableDescriptor(table);
    return descriptor.compressionBlockSize;
  }

  @Override
  public CompressionCodec getCompressionCodec(String table) {
    checkTable(table);
    TableDescriptor descriptor = getTableDescriptor(table);
    return getInstance(descriptor.compressionClass,CompressionCodec.class);
  }
  
  @Override
  public SortedSet<String> getShardListCurrentServerOnly(String table) throws IOException {
    return new TreeSet<String>(getShardsToServe(table));
  }

  @Override
  public Map<String, BlurIndex> getIndexes(String table) throws IOException {
    checkTable(table);

    Set<String> shardsToServe = getShardsToServe(table);
    synchronized (_indexes) {
      if (!_indexes.containsKey(table)) {
        _indexes.putIfAbsent(table, new ConcurrentHashMap<String, BlurIndex>());
      }
    }
    Map<String, BlurIndex> tableIndexes = _indexes.get(table);
    Set<String> shardsBeingServed = new HashSet<String>(tableIndexes.keySet());
    if (shardsBeingServed.containsAll(shardsToServe)) {
      Map<String, BlurIndex> result = new HashMap<String, BlurIndex>(tableIndexes);
      shardsBeingServed.removeAll(shardsToServe);
      for (String shardNotToServe : shardsBeingServed) {
        result.remove(shardNotToServe);
      }
      return result;
    } else {
      return openMissingShards(table, shardsToServe, tableIndexes);
    }
  }

  private BlurIndex openShard(String table, String shard) throws IOException {
    LOG.info("Opening shard [{0}] for table [{1}]", shard, table);
    Path tablePath = new Path(getTableDescriptor(table).tableUri);
    Path hdfsDirPath = new Path(tablePath, shard);

    ZookeeperLockFactory lockFactory = new ZookeeperLockFactory(_zookeeper, ZookeeperPathConstants.getLockPath(BlurConstants.BLUR_CLUSTER, table), shard, getNodeName());

    DirectIODirectory directory = new HdfsDirectory(hdfsDirPath);
    directory.setLockFactory(lockFactory);

    TableDescriptor descriptor = _clusterStatus.getTableDescriptor(true,table);
    String compressionClass = descriptor.compressionClass;
    int compressionBlockSize = descriptor.compressionBlockSize;
    if (compressionClass != null) {
      CompressionCodec compressionCodec;
      try {
        compressionCodec = CreateTable.getInstance(compressionClass,CompressionCodec.class);
        directory = new CompressedFieldDataDirectory(directory, compressionCodec, compressionBlockSize);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    
    DirectIODirectory dir;
    boolean blockCacheEnabled = _clusterStatus.isBlockCacheEnabled(table);
    if (blockCacheEnabled) {
      Set<String> blockCacheFileTypes = _clusterStatus.getBlockCacheFileTypes(table);
      dir = new BlockDirectory(table + "_" + shard, directory, _cache, blockCacheFileTypes);
    } else {
      dir = directory;
    }
    BlurIndexWriter writer = new BlurIndexWriter();
    writer.setCloser(_closer);
    writer.setCommiter(_commiter);
    writer.setAnalyzer(getAnalyzer(table));
    writer.setDirectory(dir);
    writer.setRefresher(_refresher);
    writer.setBlurMetrics(_blurMetrics);
    writer.setShard(shard);
    writer.setTable(table);
    writer.setIndexDeletionPolicy(_indexDeletionPolicy);
    writer.setSimilarity(getSimilarity(table));
    writer.setClusterStatus(_clusterStatus);
    writer.init();
    _filterCache.opening(table,shard,writer);
    return warmUp(writer,table,shard);
  }

  private BlurIndex warmUp(BlurIndex index, String table, String shard) throws IOException {
    final IndexReader reader = index.getIndexReader(true);
    warmUpAllSegments(reader);
    _warmup.warmBlurIndex(table,shard,reader,index.isClosed(), new ReleaseReader() {
      @Override
      public void release() throws IOException {
        // this will allow for closing of index
        reader.decRef();          
      }
    });
    
    return index;
  }

  private void warmUpAllSegments(IndexReader reader) throws IOException {
    IndexReader[] indexReaders = reader.getSequentialSubReaders();
    if (indexReaders != null) {
      for (IndexReader r : indexReaders) {
        warmUpAllSegments(r);
      }
    }
    int maxDoc = reader.maxDoc();
    int numDocs = reader.numDocs();
    Collection<String> fieldNames = reader.getFieldNames(FieldOption.ALL);
    Term term = new Term(PRIME_DOC, PRIME_DOC_VALUE);
    int primeDocCount = reader.docFreq(term);

    TermDocs termDocs = reader.termDocs(term);
    termDocs.next();
    termDocs.close();

    TermPositions termPositions = reader.termPositions(term);
    if (termPositions.next()) {
      if (termPositions.freq() > 0) {
        termPositions.nextPosition();
      }
    }
    termPositions.close();
    LOG.info("Warmup of indexreader [" + reader + "] complete, maxDocs [" + maxDoc + "], numDocs [" + numDocs + "], primeDocumentCount [" + primeDocCount + "], fieldCount ["
        + fieldNames.size() + "]");
  }

  private synchronized Map<String, BlurIndex> openMissingShards(final String table, Set<String> shardsToServe, final Map<String, BlurIndex> tableIndexes) {
    Map<String, Future<BlurIndex>> opening = new HashMap<String, Future<BlurIndex>>();
    for (String s : shardsToServe) {
      final String shard = s;
      BlurIndex blurIndex = tableIndexes.get(shard);
      if (blurIndex == null) {
        LOG.info("Opening missing shard [{0}] from table [{1}]", shard, table);
        Future<BlurIndex> submit = _openerService.submit(new Callable<BlurIndex>() {
          @Override
          public BlurIndex call() throws Exception {
            return openShard(table, shard);
          }
        });
        opening.put(shard, submit);
      }
    }

    for (Entry<String, Future<BlurIndex>> entry : opening.entrySet()) {
      String shard = entry.getKey();
      Future<BlurIndex> future = entry.getValue();
      try {
        BlurIndex blurIndex = future.get();
        tableIndexes.put(shard, blurIndex);
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("Unknown error while opening shard [{0}] for table [{1}].", e.getCause(), shard, table);
      }
    }

    Map<String, BlurIndex> result = new HashMap<String, BlurIndex>();
    for (String shard : shardsToServe) {
      BlurIndex blurIndex = tableIndexes.get(shard);
      if (blurIndex == null) {
        LOG.error("Missing shard [{0}] for table [{1}].", shard, table);
      } else {
        result.put(shard, blurIndex);
      }
    }
    return result;
  }

  private Set<String> getShardsToServe(String table) {
    TABLE_STATUS tableStatus = getTableStatus(table);
    if (tableStatus == TABLE_STATUS.DISABLED) {
      return new HashSet<String>();
    }
    DistributedLayoutManager layoutManager = _layoutManagers.get(table);
    if (layoutManager == null) {
      return setupLayoutManager(table);
    } else {
      return _layoutCache.get(table);
    }
  }

  private synchronized Set<String> setupLayoutManager(String table) {
    DistributedLayoutManager layoutManager = new DistributedLayoutManager();

    String cluster = _clusterStatus.getCluster(table);

    List<String> shardServerList = _clusterStatus.getShardServerList(cluster);
    List<String> offlineShardServers = new ArrayList<String>(_clusterStatus.getOfflineShardServers(cluster));
    List<String> shardList = getShardList(table);

    layoutManager.setNodes(shardServerList);
    layoutManager.setNodesOffline(offlineShardServers);
    layoutManager.setShards(shardList);
    layoutManager.init();

    Map<String, String> layout = layoutManager.getLayout();
    String nodeName = getNodeName();
    Set<String> shardsToServeCache = new TreeSet<String>();
    for (Entry<String, String> entry : layout.entrySet()) {
      if (entry.getValue().equals(nodeName)) {
        shardsToServeCache.add(entry.getKey());
      }
    }
    _layoutCache.put(table, shardsToServeCache);
    _layoutManagers.put(table, layoutManager);
    return shardsToServeCache;
  }

  @Override
  public String getNodeName() {
    return _nodeName;
  }

  @Override
  public int getShardCount(String table) {
    checkTable(table);
    TableDescriptor descriptor = getTableDescriptor(table);
    return descriptor.shardCount;
  }

  @Override
  public List<String> getShardList(String table) {
    checkTable(table);
    List<String> result = new ArrayList<String>();
    try {
      TableDescriptor descriptor = getTableDescriptor(table);
      Path tablePath = new Path(descriptor.tableUri);
      FileSystem fileSystem = FileSystem.get(tablePath.toUri(), _configuration);
      if (!fileSystem.exists(tablePath)) {
        LOG.error("Table [{0}] is missing, defined location [{1}]", table, tablePath.toUri());
        throw new RuntimeException("Table [" + table + "] is missing, defined location [" + tablePath.toUri() + "]");
      }
      FileStatus[] listStatus = fileSystem.listStatus(tablePath);
      for (FileStatus status : listStatus) {
        if (status.isDir()) {
          String name = status.getPath().getName();
          if (name.startsWith(SHARD_PREFIX)) {
            result.add(name);
          }
        }
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Similarity getSimilarity(String table) {
    checkTable(table);
    Similarity similarity = _tableSimilarity.get(table);
    if (similarity == null) {
      TableDescriptor tableDescriptor = _clusterStatus.getTableDescriptor(true,table);
      String similarityClass = tableDescriptor.similarityClass;
      if (similarityClass == null) {
        similarity = new FairSimilarity();
      } else {
        similarity = getInstance(similarityClass,Similarity.class);
      }
      _tableSimilarity.put(table, similarity);
    }
    return similarity;
  }

  @Override
  public List<String> getTableList() {
    return _clusterStatus.getTableList();
  }

  @Override
  public long getTableSize(String table) throws IOException {
    checkTable(table);
    Path tablePath = new Path(getTableUri(table));
    FileSystem fileSystem = FileSystem.get(tablePath.toUri(), _configuration);
    ContentSummary contentSummary = fileSystem.getContentSummary(tablePath);
    return contentSummary.getLength();
  }

  @Override
  public TABLE_STATUS getTableStatus(String table) {
    checkTable(table);
    boolean enabled = _clusterStatus.isEnabled(true,table);
    if (enabled) {
      return TABLE_STATUS.ENABLED;
    }
    return TABLE_STATUS.DISABLED;
  }

  private void checkTable(String table) {
    if (_clusterStatus.exists(true,table)) {
      return;
    }
    throw new RuntimeException("Table [" + table + "] does not exist.");
  }

  @Override
  public String getTableUri(String table) {
    checkTable(table);
    TableDescriptor descriptor = getTableDescriptor(table);
    return descriptor.tableUri;
  }

  private TableDescriptor getTableDescriptor(String table) {
    TableDescriptor tableDescriptor = _tableDescriptors.get(table);
    if (tableDescriptor == null) {
      tableDescriptor = _clusterStatus.getTableDescriptor(true,table);
      _tableDescriptors.put(table, tableDescriptor);
    }
    return tableDescriptor;
  }

  @SuppressWarnings("unchecked")
  private <T> T getInstance(String className, Class<T> c) {
    try {
      Class<? extends T> clazz = (Class<? extends T>) Class.forName(className);
      Object object = clazz.newInstance();
      if (object instanceof Configurable) {
        Configurable configurable = (Configurable) object;
        configurable.setConf(_configuration);
      }
      return (T) object;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void setClusterStatus(ClusterStatus clusterStatus) {
    _clusterStatus = clusterStatus;
  }

  public void setConfiguration(Configuration configuration) {
    _configuration = configuration;
  }

  public void setNodeName(String nodeName) {
    _nodeName = nodeName;
  }

  public void setShardOpenerThreadCount(int shardOpenerThreadCount) {
    _shardOpenerThreadCount = shardOpenerThreadCount;
  }

  public void setRefresher(BlurIndexRefresher refresher) {
    _refresher = refresher;
  }

  public void setCommiter(BlurIndexCommiter commiter) {
    _commiter = commiter;
  }

  public void setCache(BlockDirectoryCache cache) {
    _cache = cache;
  }

  public void setBlurMetrics(BlurMetrics blurMetrics) {
    _blurMetrics = blurMetrics;
  }

  public void setCloser(BlurIndexCloser closer) {
    _closer = closer;
  }

  public void setZookeeper(ZooKeeper zookeeper) {
    _zookeeper = zookeeper;
  }

  public void setFilterCache(BlurFilterCache filterCache) {
    _filterCache = filterCache;
  }

  public void setSafeModeDelay(long safeModeDelay) {
    _safeModeDelay = safeModeDelay;
  }

  public void setWarmup(BlurIndexWarmup warmup) {
    _warmup = warmup;
  }

  public void setIndexDeletionPolicy(IndexDeletionPolicy indexDeletionPolicy) {
    _indexDeletionPolicy = indexDeletionPolicy;
  }
}
