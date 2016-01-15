package org.apache.blur.manager.indexserver;

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
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.manager.writer.BlurIndexCloser;
import org.apache.blur.manager.writer.BlurIndexConf;
import org.apache.blur.manager.writer.BlurIndexSimpleWriter;
import org.apache.blur.manager.writer.SharedMergeScheduler;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.store.hdfs.BlurLockFactory;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.ShardUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;

import com.google.common.io.Closer;

public class LocalIndexServer extends AbstractIndexServer {

  private final static Log LOG = LogFactory.getLog(LocalIndexServer.class);

  private final Map<String, Map<String, BlurIndex>> _readersMap = new ConcurrentHashMap<String, Map<String, BlurIndex>>();
  private final SharedMergeScheduler _mergeScheduler;
  private final ExecutorService _searchExecutor;
  private final TableContext _tableContext;
  private final Closer _closer;
  private final BlurIndexCloser _indexCloser;
  private final Timer _timer;
  private final Timer _bulkTimer;
  private final Timer _closerTimer;
  private final long _maxWriterIdle;

  public LocalIndexServer(TableDescriptor tableDescriptor) throws IOException {
    _timer = new Timer("Index Importer", true);
    _bulkTimer = new Timer("Bulk Indexing", true);
    _closerTimer = new Timer("Writer Closer", true);
    _maxWriterIdle = TimeUnit.SECONDS.toMillis(30);
    _closer = Closer.create();
    _tableContext = TableContext.create(tableDescriptor);
    _mergeScheduler = _closer.register(new SharedMergeScheduler(3, 128 * 1000 * 1000));
    _searchExecutor = Executors.newCachedThreadPool();
    _closer.register(new CloseableExecutorService(_searchExecutor));
    _indexCloser = _closer.register(new BlurIndexCloser());
    _closer.register(new Closeable() {
      @Override
      public void close() throws IOException {
        _timer.cancel();
        _timer.purge();
      }
    });
    getIndexes(_tableContext.getTable());
  }

  @Override
  public void close() {
    try {
      _closer.close();
    } catch (IOException e) {
      LOG.error("Unknown error", e);
    }
    for (String table : _readersMap.keySet()) {
      close(_readersMap.get(table));
    }
  }

  @Override
  public SortedSet<String> getShardListCurrentServerOnly(String table) throws IOException {
    Map<String, BlurIndex> tableMap = _readersMap.get(table);
    Set<String> shardsSet;
    if (tableMap == null) {
      shardsSet = getIndexes(table).keySet();
    } else {
      shardsSet = tableMap.keySet();
    }
    return new TreeSet<String>(shardsSet);
  }

  @Override
  public Map<String, BlurIndex> getIndexes(String table) throws IOException {
    Map<String, BlurIndex> tableMap = _readersMap.get(table);
    if (tableMap == null) {
      tableMap = openFromDisk();
      _readersMap.put(table, tableMap);
    }
    return tableMap;
  }

  private void close(Map<String, BlurIndex> map) {
    for (BlurIndex index : map.values()) {
      try {
        index.close();
      } catch (Exception e) {
        LOG.error("Error while trying to close index.", e);
      }
    }
  }

  private Map<String, BlurIndex> openFromDisk() throws IOException {
    String table = _tableContext.getDescriptor().getName();
    Path tablePath = _tableContext.getTablePath();
    Configuration configuration = _tableContext.getConfiguration();
    FileSystem fileSystem = tablePath.getFileSystem(configuration);
    if (fileSystem.isDirectory(tablePath)) {
      Map<String, BlurIndex> shards = new ConcurrentHashMap<String, BlurIndex>();
      int shardCount = _tableContext.getDescriptor().getShardCount();
      for (int i = 0; i < shardCount; i++) {
        String shardName = ShardUtil.getShardName(BlurConstants.SHARD_PREFIX, i);
        Path hdfsPath = new Path(tablePath, shardName);
        Directory directory = new HdfsDirectory(configuration, hdfsPath);
        BlurLockFactory lockFactory = new BlurLockFactory(configuration, hdfsPath, getNodeName(), BlurUtil.getPid());
        directory.setLockFactory(lockFactory);
        shards.put(shardName, openIndex(table, shardName, directory));
      }
      return shards;
    }
    throw new IOException("Table [" + table + "] not found.");
  }

  private BlurIndex openIndex(String table, String shard, Directory dir) throws CorruptIndexException, IOException {
    ShardContext shardContext = ShardContext.create(_tableContext, shard);
    BlurIndexSimpleWriter index = new BlurIndexSimpleWriter(new BlurIndexConf(shardContext, dir, _mergeScheduler,
        _searchExecutor, _indexCloser, _timer, _bulkTimer, null, _closerTimer, _maxWriterIdle));
    return index;
  }

  @Override
  public String getNodeName() {
    return "localhost";
  }

  @Override
  public long getTableSize(String table) throws IOException {
    try {
      File file = new File(new URI(_tableContext.getTablePath().toUri().toString()));
      return getFolderSize(file);
    } catch (URISyntaxException e) {
      throw new IOException("bad URI", e);
    }
  }

  private long getFolderSize(File file) {
    long size = 0;
    if (file.isDirectory()) {
      for (File sub : file.listFiles()) {
        size += getFolderSize(sub);
      }
    } else {
      size += file.length();
    }
    return size;
  }

  @Override
  public Map<String, ShardState> getShardState(String table) {
    throw new RuntimeException("Not supported yet.");
  }

  @Override
  public long getSegmentImportInProgressCount(String table) {
    return 0l;
  }

  @Override
  public long getSegmentImportPendingCount(String table) {
    return 0l;
  }
}
