/**
s * Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.apache.blur.manager.writer;

import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;
import static org.apache.blur.utils.BlurConstants.ACL_DISCOVER;
import static org.apache.blur.utils.BlurConstants.ACL_READ;
import static org.apache.blur.utils.BlurConstants.BLUR_RECORD_SECURITY_DEFAULT_READMASK_MESSAGE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_WRITER_SORT_FACTOR;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_WRITER_SORT_MEMORY;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_QUEUE_MAX_INMEMORY_LENGTH;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.analysis.FieldManager;
import org.apache.blur.index.ExitableReader;
import org.apache.blur.index.IndexDeletionPolicyReader;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.codec.Blur024Codec;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.lucene.search.IndexSearcherCloseableBase;
import org.apache.blur.lucene.search.SuperQuery;
import org.apache.blur.lucene.security.index.AccessControlFactory;
import org.apache.blur.memory.MemoryLeakDetector;
import org.apache.blur.server.IndexSearcherCloseableSecureBase;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.server.cache.ThriftCache;
import org.apache.blur.store.hdfs_v2.StoreDirection;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Sorter;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BlurIndexWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import com.google.common.base.Splitter;

public class BlurIndexSimpleWriter extends BlurIndex {

  private static final String TRUE = "true";

  private static final Log LOG = LogFactory.getLog(BlurIndexSimpleWriter.class);

  private final AtomicBoolean _isClosed = new AtomicBoolean();
  private final BlurIndexCloser _indexCloser;
  private final AtomicReference<DirectoryReader> _indexReader = new AtomicReference<DirectoryReader>();
  private final ExecutorService _searchThreadPool;
  private final Directory _directory;
  private final IndexWriterConfig _conf;
  private final TableContext _tableContext;
  private final FieldManager _fieldManager;
  private final ShardContext _shardContext;
  private final AtomicReference<BlurIndexWriter> _writer = new AtomicReference<BlurIndexWriter>();
  private final boolean _makeReaderExitable = true;
  private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
  private final WriteLock _writeLock = _lock.writeLock();
  private final ReadWriteLock _indexRefreshLock = new ReentrantReadWriteLock();
  private final Lock _indexRefreshWriteLock = _indexRefreshLock.writeLock();
  private final Lock _indexRefreshReadLock = _indexRefreshLock.readLock();
  private final IndexDeletionPolicyReader _policy;
  private final SnapshotIndexDeletionPolicy _snapshotIndexDeletionPolicy;
  private final String _context;
  private final AtomicInteger _writesWaiting = new AtomicInteger();
  private final BlockingQueue<RowMutation> _queue;
  private final MutationQueueProcessor _mutationQueueProcessor;
  private final Timer _indexImporterTimer;
  private final Map<String, BulkEntry> _bulkWriters;
  private final boolean _security;
  private final AccessControlFactory _accessControlFactory;
  private final Set<String> _discoverableFields;
  private final Splitter _commaSplitter;
  private final Timer _bulkIndexingTimer;
  private final TimerTask _watchForIdleBulkWriters;
  private final ThriftCache _thriftCache;
  private final String _defaultReadMaskMessage;
  private final IndexImporter _indexImporter;
  private final Timer _indexWriterTimer;
  private final AtomicLong _lastWrite = new AtomicLong();
  private final long _maxWriterIdle;
  private final TimerTask _watchForIdleWriter;

  private volatile Thread _optimizeThread;

  public BlurIndexSimpleWriter(BlurIndexConfig blurIndexConf) throws IOException {
    super(blurIndexConf);
    _maxWriterIdle = blurIndexConf.getMaxWriterIdle();
    _indexWriterTimer = blurIndexConf.getIndexWriterTimer();
    _thriftCache = blurIndexConf.getThriftCache();
    _commaSplitter = Splitter.on(',');
    _bulkWriters = new ConcurrentHashMap<String, BlurIndexSimpleWriter.BulkEntry>();
    _indexImporterTimer = blurIndexConf.getIndexImporterTimer();
    _bulkIndexingTimer = blurIndexConf.getBulkIndexingTimer();
    _searchThreadPool = blurIndexConf.getSearchExecutor();
    _shardContext = blurIndexConf.getShardContext();
    _tableContext = _shardContext.getTableContext();
    _context = _tableContext.getTable() + "/" + _shardContext.getShard();
    _fieldManager = _tableContext.getFieldManager();
    _discoverableFields = _tableContext.getDiscoverableFields();
    _accessControlFactory = _tableContext.getAccessControlFactory();
    _defaultReadMaskMessage = getDefaultReadMaskMessage(_tableContext);

    TableDescriptor descriptor = _tableContext.getDescriptor();
    Map<String, String> tableProperties = descriptor.getTableProperties();
    if (tableProperties != null) {
      String value = tableProperties.get(BlurConstants.BLUR_RECORD_SECURITY);
      if (value != null && value.equals(TRUE)) {
        LOG.info("Record Level Security has been enabled for table [{0}] shard [{1}]", _tableContext.getTable(),
            _shardContext.getShard());
        _security = true;
      } else {
        _security = false;
      }
    } else {
      _security = false;
    }
    Analyzer analyzer = _fieldManager.getAnalyzerForIndex();
    _conf = new IndexWriterConfig(LUCENE_VERSION, analyzer);
    _conf.setWriteLockTimeout(TimeUnit.MINUTES.toMillis(5));
    _conf.setCodec(new Blur024Codec(_tableContext.getBlurConfiguration()));
    _conf.setSimilarity(_tableContext.getSimilarity());
    _conf.setInfoStream(new LoggingInfoStream(_tableContext.getTable(), _shardContext.getShard()));
    TieredMergePolicy mergePolicy = (TieredMergePolicy) _conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    _conf.setMergeScheduler(blurIndexConf.getMergeScheduler().getMergeScheduler());
    _snapshotIndexDeletionPolicy = new SnapshotIndexDeletionPolicy(_tableContext.getConfiguration(),
        SnapshotIndexDeletionPolicy.getGenerationsPath(_shardContext.getHdfsDirPath()));
    _policy = new IndexDeletionPolicyReader(_snapshotIndexDeletionPolicy);
    _conf.setIndexDeletionPolicy(_policy);
    BlurConfiguration blurConfiguration = _tableContext.getBlurConfiguration();
    _queue = new ArrayBlockingQueue<RowMutation>(blurConfiguration.getInt(BLUR_SHARD_QUEUE_MAX_INMEMORY_LENGTH, 100));
    _mutationQueueProcessor = new MutationQueueProcessor(_queue, this, _shardContext, _writesWaiting);

    _directory = blurIndexConf.getDirectory();
    if (!DirectoryReader.indexExists(_directory)) {
      new BlurIndexWriter(_directory, _conf).close();
    }

    _indexCloser = blurIndexConf.getIndexCloser();
    DirectoryReader realDirectoryReader = DirectoryReader.open(_directory);
    DirectoryReader wrappped = wrap(realDirectoryReader);
    String message = "BlurIndexSimpleWriter - inital open";
    DirectoryReader directoryReader = checkForMemoryLeaks(wrappped, message);
    _indexReader.set(directoryReader);

    _indexImporter = new IndexImporter(_indexImporterTimer, BlurIndexSimpleWriter.this, _shardContext,
        TimeUnit.SECONDS, 10, 120, _thriftCache, _directory);

    _watchForIdleBulkWriters = new TimerTask() {
      @Override
      public void run() {
        try {
          watchForIdleBulkWriters();
        } catch (Throwable t) {
          LOG.error("Unknown error.", t);
        }
      }

      private void watchForIdleBulkWriters() {
        for (BulkEntry bulkEntry : _bulkWriters.values()) {
          bulkEntry._lock.lock();
          try {
            if (!bulkEntry.isClosed() && bulkEntry.isIdle()) {
              LOG.info("Bulk Entry [{0}] has become idle and now closing.", bulkEntry);
              try {
                bulkEntry.close();
              } catch (IOException e) {
                LOG.error("Unkown error while trying to close bulk writer when it became idle.", e);
              }
            }
          } finally {
            bulkEntry._lock.unlock();
          }
        }
      }
    };
    long delay = TimeUnit.SECONDS.toMillis(30);
    _bulkIndexingTimer.schedule(_watchForIdleBulkWriters, delay, delay);
    _watchForIdleWriter = new TimerTask() {
      @Override
      public void run() {
        try {
          closeWriter();
        } catch (Throwable t) {
          LOG.error("Unknown error while trying to close idle writer.", t);
        }
      }
    };
    _indexWriterTimer.schedule(_watchForIdleWriter, _maxWriterIdle, _maxWriterIdle);
  }

  public int getReaderGenerationCount() {
    return _policy.getReaderGenerationCount();
  }

  private String getDefaultReadMaskMessage(TableContext tableContext) {
    BlurConfiguration blurConfiguration = tableContext.getBlurConfiguration();
    String message = blurConfiguration.get(BLUR_RECORD_SECURITY_DEFAULT_READMASK_MESSAGE);
    if (message == null || message.trim().isEmpty()) {
      return null;
    }
    return message.trim();
  }

  private DirectoryReader checkForMemoryLeaks(DirectoryReader wrappped, String message) {
    DirectoryReader directoryReader = MemoryLeakDetector.record(wrappped, message, _tableContext.getTable(),
        _shardContext.getShard());
    if (directoryReader instanceof ExitableReader) {
      ExitableReader exitableReader = (ExitableReader) directoryReader;
      checkForMemoryLeaks(exitableReader.getIn().leaves(), message);
    } else {
      checkForMemoryLeaks(directoryReader.leaves(), message);
    }
    return directoryReader;
  }

  private void checkForMemoryLeaks(List<AtomicReaderContext> leaves, String message) {
    for (AtomicReaderContext context : leaves) {
      AtomicReader reader = context.reader();
      MemoryLeakDetector.record(reader, message, _tableContext.getTable(), _shardContext.getShard());
    }
  }

  private DirectoryReader wrap(DirectoryReader reader) throws IOException {
    if (_makeReaderExitable) {
      reader = new ExitableReader(reader);
    }
    return _policy.register(reader);
  }

  @Override
  public IndexSearcherCloseable getIndexSearcher() throws IOException {
    return getIndexSearcher(_security);
  }

  public IndexSearcherCloseable getIndexSearcher(boolean security) throws IOException {
    final IndexReader indexReader;
    _indexRefreshReadLock.lock();
    try {
      indexReader = _indexReader.get();
      indexReader.incRef();
    } finally {
      _indexRefreshReadLock.unlock();
    }
    if (indexReader instanceof ExitableReader) {
      ((ExitableReader) indexReader).reset();
    }
    if (security) {
      return getSecureIndexSearcher(indexReader);
    } else {
      return getInsecureIndexSearcher(indexReader);
    }
  }

  private IndexSearcherCloseable getSecureIndexSearcher(final IndexReader indexReader) throws IOException {
    String readStr = null;
    String discoverStr = null;
    User user = UserContext.getUser();
    if (user != null) {
      Map<String, String> attributes = user.getAttributes();
      if (attributes != null) {
        readStr = attributes.get(ACL_READ);
        discoverStr = attributes.get(ACL_DISCOVER);
      }
    }
    Collection<String> readAuthorizations = toCollection(readStr);
    Collection<String> discoverAuthorizations = toCollection(discoverStr);
    return new IndexSearcherCloseableSecureBase(indexReader, _searchThreadPool, _accessControlFactory,
        readAuthorizations, discoverAuthorizations, _discoverableFields, _defaultReadMaskMessage) {
      private boolean _closed;

      @Override
      public Directory getDirectory() {
        return _directory;
      }

      @Override
      public synchronized void close() throws IOException {
        if (!_closed) {
          indexReader.decRef();
          _closed = true;
        } else {
          // Not really sure why some indexes get closed called twice on them.
          // This is in place to log it.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Searcher already closed [{0}].", new Throwable(), this);
          }
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  private Collection<String> toCollection(String aclStr) {
    if (aclStr == null) {
      return Collections.EMPTY_LIST;
    }
    Set<String> result = new HashSet<String>();
    for (String s : _commaSplitter.split(aclStr)) {
      result.add(s);
    }
    return result;
  }

  private IndexSearcherCloseable getInsecureIndexSearcher(final IndexReader indexReader) {
    return new IndexSearcherCloseableBase(indexReader, _searchThreadPool) {
      private boolean _closed;

      @Override
      public Directory getDirectory() {
        return _directory;
      }

      @Override
      public synchronized void close() throws IOException {
        if (!_closed) {
          indexReader.decRef();
          _closed = true;
        } else {
          // Not really sure why some indexes get closed called twice on them.
          // This is in place to log it.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Searcher already closed [{0}].", new Throwable(), this);
          }
        }
      }
    };
  }

  @Override
  public void close() throws IOException {
    _isClosed.set(true);
    IOUtils.cleanup(LOG, makeCloseable(_bulkIndexingTimer, _watchForIdleBulkWriters),
        makeCloseable(_indexWriterTimer, _watchForIdleWriter), _indexImporter, _mutationQueueProcessor,
        makeCloseable(_writer.get()), _indexReader.get(), _directory);
  }

  private Closeable makeCloseable(final BlurIndexWriter blurIndexWriter) {
    return new Closeable() {
      @Override
      public void close() throws IOException {
        if (blurIndexWriter != null) {
          blurIndexWriter.close(false);
        }
      }
    };
  }

  private Closeable makeCloseable(Timer timer, final TimerTask timerTask) {
    return new Closeable() {
      @Override
      public void close() throws IOException {
        timerTask.cancel();
        timer.purge();
      }
    };
  }

  @Override
  public AtomicBoolean isClosed() {
    return _isClosed;
  }

  private void closeWriter() {
    if (_lastWrite.get() + _maxWriterIdle < System.currentTimeMillis()) {
      synchronized (_writer) {
        _writeLock.lock();
        try {
          BlurIndexWriter writer = _writer.getAndSet(null);
          if (writer != null) {
            LOG.info("Closing idle writer for table [{0}] shard [{1}]", _tableContext.getTable(),
                _shardContext.getShard());
            IOUtils.cleanup(LOG, writer);
          }
        } finally {
          _writeLock.unlock();
        }
      }
    }
  }

  protected boolean isWriterClosed() {
    synchronized (_writer) {
      return _writer.get() == null;
    }
  }

  private BlurIndexWriter getBlurIndexWriter() throws IOException {
    synchronized (_writer) {
      BlurIndexWriter blurIndexWriter = _writer.get();
      if (blurIndexWriter == null) {
        blurIndexWriter = new BlurIndexWriter(_directory, _conf.clone());
        _writer.set(blurIndexWriter);
        _lastWrite.set(System.currentTimeMillis());
      }
      return blurIndexWriter;
    }
  }

  private void resetBlurIndexWriter() {
    synchronized (_writer) {
      _writer.set(null);
    }
  }

  @Override
  public synchronized void optimize(final int numberOfSegmentsPerShard) throws IOException {
    final String table = _tableContext.getTable();
    final String shard = _shardContext.getShard();
    if (_optimizeThread != null && _optimizeThread.isAlive()) {
      LOG.info("Already running an optimize on table [{0}] shard [{1}]", table, shard);
      return;
    }
    _optimizeThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          BlurIndexWriter writer = getBlurIndexWriter();
          writer.forceMerge(numberOfSegmentsPerShard, true);
          _writeLock.lock();
          try {
            commit();
          } finally {
            _writeLock.unlock();
          }
        } catch (Exception e) {
          LOG.error("Unknown error during optimize on table [{0}] shard [{1}]", e, table, shard);
        }
      }
    });
    _optimizeThread.setDaemon(true);
    _optimizeThread.setName("Optimize table [" + table + "] shard [" + shard + "]");
    _optimizeThread.start();
  }

  @Override
  public void createSnapshot(String name) throws IOException {
    _writeLock.lock();
    try {
      _snapshotIndexDeletionPolicy.createSnapshot(name, _indexReader.get(), _context);
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public void removeSnapshot(String name) throws IOException {
    _writeLock.lock();
    try {
      _snapshotIndexDeletionPolicy.removeSnapshot(name, _context);
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public List<String> getSnapshots() throws IOException {
    return new ArrayList<String>(_snapshotIndexDeletionPolicy.getSnapshots());
  }

  private void commit() throws IOException {
    Tracer trace1 = Trace.trace("prepareCommit");
    BlurIndexWriter writer = getBlurIndexWriter();
    writer.prepareCommit();
    trace1.done();

    Tracer trace2 = Trace.trace("commit");
    writer.commit();
    trace2.done();

    Tracer trace3 = Trace.trace("index refresh");
    DirectoryReader currentReader = _indexReader.get();
    DirectoryReader newReader = DirectoryReader.openIfChanged(currentReader);
    if (newReader == null) {
      LOG.debug("Reader should be new after commit for table [{0}] shard [{1}].", _tableContext.getTable(),
          _shardContext.getShard());
    } else {
      DirectoryReader reader = wrap(newReader);
      checkForMemoryLeaks(reader, "BlurIndexSimpleWriter - reopen table [{0}] shard [{1}]");
      _indexRefreshWriteLock.lock();
      try {
        _indexReader.set(reader);
      } finally {
        _indexRefreshWriteLock.unlock();
      }
      _indexCloser.close(currentReader);
    }
    trace3.done();
  }

  @Override
  public void process(IndexAction indexAction) throws IOException {
    _writesWaiting.incrementAndGet();
    _writeLock.lock();
    _writesWaiting.decrementAndGet();
    indexAction.setWritesWaiting(_writesWaiting);
    BlurIndexWriter writer = getBlurIndexWriter();
    IndexSearcherCloseable indexSearcher = null;
    try {
      indexSearcher = getIndexSearcher(false);
      indexAction.performMutate(indexSearcher, writer);
      indexAction.doPreCommit(indexSearcher, writer);
      commit();
      indexAction.doPostCommit(writer);
    } catch (Exception e) {
      indexAction.doPreRollback(writer);
      writer.rollback();
      resetBlurIndexWriter();
      indexAction.doPostRollback(writer);
      throw new IOException("Unknown error during mutation", e);
    } finally {
      if (_thriftCache != null) {
        _thriftCache.clearTable(_tableContext.getTable());
      }
      if (indexSearcher != null) {
        indexSearcher.close();
      }
      _lastWrite.set(System.currentTimeMillis());
      _writeLock.unlock();
    }
  }

  public Path getSnapshotsDirectoryPath() {
    return _snapshotIndexDeletionPolicy.getSnapshotsDirectoryPath();
  }

  @Override
  public void enqueue(List<RowMutation> mutations) throws IOException {
    startQueueIfNeeded();
    try {
      for (RowMutation mutation : mutations) {
        _queue.put(mutation);
      }
      synchronized (_queue) {
        _queue.notifyAll();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void startQueueIfNeeded() {
    _mutationQueueProcessor.startIfNotRunning();
  }

  static class BulkEntry {

    private final long _idleTime = TimeUnit.SECONDS.toNanos(30);
    private final Path _parentPath;
    private final String _bulkId;
    private final TableContext _tableContext;
    private final ShardContext _shardContext;
    private final Configuration _configuration;
    private final FileSystem _fileSystem;
    private final String _table;
    private final String _shard;
    private final Lock _lock = new ReentrantReadWriteLock().writeLock();

    private volatile SequenceFile.Writer _writer;
    private volatile long _lastWrite;
    private volatile int _count = 0;

    public BulkEntry(String bulkId, Path parentPath, ShardContext shardContext) throws IOException {
      _bulkId = bulkId;
      _parentPath = parentPath;
      _shardContext = shardContext;
      _tableContext = shardContext.getTableContext();
      _configuration = _tableContext.getConfiguration();
      _fileSystem = _parentPath.getFileSystem(_configuration);
      _shard = _shardContext.getShard();
      _table = _tableContext.getTable();
    }

    public boolean isClosed() {
      return _writer == null;
    }

    private Writer openSeqWriter() throws IOException {
      Progressable progress = new Progressable() {
        @Override
        public void progress() {

        }
      };
      final CompressionCodec codec;
      final CompressionType type;

      if (isSnappyCodecLoaded(_configuration)) {
        codec = new SnappyCodec();
        type = CompressionType.BLOCK;
      } else {
        codec = new DefaultCodec();
        type = CompressionType.NONE;
      }

      Path path = new Path(_parentPath, _shard + "." + _count + ".unsorted.seq");

      _count++;

      return SequenceFile.createWriter(_fileSystem, _configuration, path, Text.class, RowMutationWritable.class, type,
          codec, progress);
    }

    public void close() throws IOException {
      _lock.lock();
      try {
        if (_writer != null) {
          _writer.close();
          _writer = null;
        }
      } finally {
        _lock.unlock();
      }
    }

    public void append(Text key, RowMutationWritable rowMutationWritable) throws IOException {
      _lock.lock();
      try {
        getWriter().append(key, rowMutationWritable);
        _lastWrite = System.nanoTime();
      } finally {
        _lock.unlock();
      }
    }

    private SequenceFile.Writer getWriter() throws IOException {
      if (_writer == null) {
        _writer = openSeqWriter();
        _lastWrite = System.nanoTime();
      }
      return _writer;
    }

    public boolean isIdle() {
      if (_lastWrite + _idleTime < System.nanoTime()) {
        return true;
      }
      return false;
    }

    public List<Path> getUnsortedFiles() throws IOException {
      FileStatus[] listStatus = _fileSystem.listStatus(_parentPath, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.getName().matches(_shard + "\\.[0-9].*\\.unsorted\\.seq");
        }
      });

      List<Path> unsortedPaths = new ArrayList<Path>();
      for (FileStatus fileStatus : listStatus) {
        unsortedPaths.add(fileStatus.getPath());
      }
      return unsortedPaths;
    }

    public void cleanupFiles(List<Path> unsortedPaths, Path sorted) throws IOException {
      if (unsortedPaths != null) {
        for (Path p : unsortedPaths) {
          _fileSystem.delete(p, false);
        }
      }
      if (sorted != null) {
        _fileSystem.delete(sorted, false);
      }
      removeParentIfLastFile(_fileSystem, _parentPath);
    }

    public IndexAction getIndexAction() throws IOException {
      return new IndexAction() {
        private Path _sorted;
        private List<Path> _unsortedPaths;

        @Override
        public void performMutate(IndexSearcherCloseable searcher, IndexWriter writer) throws IOException {
          Configuration configuration = _tableContext.getConfiguration();

          BlurConfiguration blurConfiguration = _tableContext.getBlurConfiguration();

          SequenceFile.Sorter sorter = new Sorter(_fileSystem, Text.class, RowMutationWritable.class, configuration);
          // This should support up to ~100 GB per shard, probably have
          // incremental updates in that batch size.
          sorter.setFactor(blurConfiguration.getInt(BLUR_SHARD_INDEX_WRITER_SORT_FACTOR, 10000));
          sorter.setMemory(blurConfiguration.getInt(BLUR_SHARD_INDEX_WRITER_SORT_MEMORY, 10 * 1024 * 1024));

          _unsortedPaths = getUnsortedFiles();

          _sorted = new Path(_parentPath, _shard + ".sorted.seq");

          LOG.info("Shard [{2}/{3}] Id [{4}] Sorting mutates paths [{0}] sorted path [{1}]", _unsortedPaths, _sorted,
              _table, _shard, _bulkId);
          sorter.sort(_unsortedPaths.toArray(new Path[_unsortedPaths.size()]), _sorted, true);

          LOG.info("Shard [{1}/{2}] Id [{3}] Applying mutates sorted path [{0}]", _sorted, _table, _shard, _bulkId);
          Reader reader = new SequenceFile.Reader(_fileSystem, _sorted, configuration);

          Text key = new Text();
          RowMutationWritable value = new RowMutationWritable();

          Text last = null;
          List<RowMutation> list = new ArrayList<RowMutation>();
          while (reader.next(key, value)) {
            if (!key.equals(last)) {
              flushMutates(searcher, writer, list);
              last = new Text(key);
              list.clear();
            }
            list.add(value.getRowMutation().deepCopy());
          }
          flushMutates(searcher, writer, list);
          reader.close();
          LOG.info("Shard [{0}/{1}] Id [{2}] Finished applying mutates starting commit.", _table, _shard, _bulkId);
        }

        private void flushMutates(IndexSearcherCloseable searcher, IndexWriter writer, List<RowMutation> list)
            throws IOException {
          if (!list.isEmpty()) {
            List<RowMutation> reduceMutates;
            try {
              reduceMutates = MutatableAction.reduceMutates(list);
            } catch (BlurException e) {
              throw new IOException(e);
            }
            for (RowMutation mutation : reduceMutates) {
              MutatableAction mutatableAction = new MutatableAction(_shardContext);
              mutatableAction.mutate(mutation);
              mutatableAction.performMutate(searcher, writer);
            }
          }
        }

        @Override
        public void doPreRollback(IndexWriter writer) throws IOException {

        }

        @Override
        public void doPreCommit(IndexSearcherCloseable indexSearcher, IndexWriter writer) throws IOException {

        }

        @Override
        public void doPostRollback(IndexWriter writer) throws IOException {
          cleanupFiles(_unsortedPaths, _sorted);
        }

        @Override
        public void doPostCommit(IndexWriter writer) throws IOException {
          cleanupFiles(_unsortedPaths, _sorted);
        }
      };
    }

    @Override
    public String toString() {
      return "BulkEntry [_bulkId=" + _bulkId + ", _table=" + _table + ", _shard=" + _shard + ", _idleTime=" + _idleTime
          + ", _lastWrite=" + _lastWrite + ", _count=" + _count + "]";
    }

  }

  public synchronized BulkEntry startBulkMutate(String bulkId) throws IOException {
    BulkEntry bulkEntry = _bulkWriters.get(bulkId);
    if (bulkEntry == null) {
      Path tablePath = _tableContext.getTablePath();
      Path bulk = new Path(tablePath, "bulk");
      Path bulkInstance = new Path(bulk, bulkId);
      Path path = new Path(bulkInstance, _shardContext.getShard() + ".notsorted.seq");

      bulkEntry = new BulkEntry(bulkId, path, _shardContext);
      _bulkWriters.put(bulkId, bulkEntry);
    } else {
      LOG.info("Bulk [{0}] mutate already started on shard [{1}] in table [{2}].", bulkId, _shardContext.getShard(),
          _tableContext.getTable());
    }
    return bulkEntry;
  }

  @Override
  public void finishBulkMutate(final String bulkId, boolean apply, boolean blockUntilComplete) throws IOException {
    final String table = _tableContext.getTable();
    final String shard = _shardContext.getShard();

    final BulkEntry bulkEntry = _bulkWriters.get(bulkId);
    if (bulkEntry == null) {
      LOG.info("Shard [{2}/{3}] Id [{0}] Nothing to apply.", bulkId, apply, table, shard);
      return;
    }
    LOG.info("Shard [{2}/{3}] Id [{0}] Finishing bulk mutate apply [{1}]", bulkId, apply, table, shard);
    bulkEntry.close();

    if (!apply) {
      bulkEntry.cleanupFiles(bulkEntry.getUnsortedFiles(), null);
    } else {
      final IndexAction indexAction = bulkEntry.getIndexAction();
      if (blockUntilComplete) {
        StoreDirection.LONG_TERM.set(true);
        try {
          process(indexAction);
        } finally {
          StoreDirection.LONG_TERM.set(false);
        }
      } else {
        Thread thread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              StoreDirection.LONG_TERM.set(true);
              process(indexAction);
            } catch (IOException e) {
              LOG.error("Shard [{0}/{1}] Id [{2}] Unknown error while trying to finish the bulk updates.", e, table,
                  shard, bulkId);
            } finally {
              StoreDirection.LONG_TERM.set(false);
            }
          }
        });
        thread.setName("Bulk Finishing Thread Table [" + table + "] Shard [" + shard + "] BulkId [" + bulkId + "]");
        thread.start();
      }
    }
  }

  @Override
  public void addBulkMutate(String bulkId, RowMutation mutation) throws IOException {
    BulkEntry bulkEntry = _bulkWriters.get(bulkId);
    if (bulkEntry == null) {
      bulkEntry = startBulkMutate(bulkId);
    }
    RowMutationWritable rowMutationWritable = new RowMutationWritable();
    rowMutationWritable.setRowMutation(mutation);
    bulkEntry.append(getKey(mutation), rowMutationWritable);
  }

  private Text getKey(RowMutation mutation) {
    return new Text(mutation.getRowId());
  }

  private static void removeParentIfLastFile(final FileSystem fileSystem, Path parent) throws IOException {
    FileStatus[] listStatus = fileSystem.listStatus(parent);
    if (listStatus != null) {
      if (listStatus.length == 0) {
        if (!fileSystem.delete(parent, false)) {
          if (fileSystem.exists(parent)) {
            LOG.error("Could not remove parent directory [{0}]", parent);
          }
        }
      }
    }
  }

  @Override
  public long getRecordCount() throws IOException {
    IndexSearcherCloseable searcher = getIndexSearcher(false);
    try {
      return searcher.getIndexReader().numDocs();
    } finally {
      if (searcher != null) {
        searcher.close();
      }
    }
  }

  @Override
  public long getRowCount() throws IOException {
    IndexSearcherCloseable searcher = getIndexSearcher(false);
    try {
      return getRowCount(searcher);
    } finally {
      if (searcher != null) {
        searcher.close();
      }
    }
  }

  protected long getRowCount(IndexSearcherCloseable searcher) throws IOException {
    TopDocs topDocs = searcher.search(
        new SuperQuery(new MatchAllDocsQuery(), ScoreType.CONSTANT, _tableContext.getDefaultPrimeDocTerm()), 1);
    return topDocs.totalHits;
  }

  @Override
  public long getIndexMemoryUsage() throws IOException {
    return 0;
  }

  @Override
  public long getSegmentCount() throws IOException {
    IndexSearcherCloseable indexSearcherClosable = getIndexSearcher(false);
    try {
      IndexReader indexReader = indexSearcherClosable.getIndexReader();
      IndexReaderContext context = indexReader.getContext();
      return context.leaves().size();
    } finally {
      indexSearcherClosable.close();
    }
  }

  private static boolean isSnappyCodecLoaded(Configuration configuration) {
    try {
      Method methodHadoop1 = SnappyCodec.class.getMethod("isNativeSnappyLoaded", new Class[] { Configuration.class });
      Boolean loaded = (Boolean) methodHadoop1.invoke(null, new Object[] { configuration });
      if (loaded != null && loaded) {
        LOG.info("Using SnappyCodec");
        return true;
      } else {
        LOG.info("Not using SnappyCodec");
        return false;
      }
    } catch (NoSuchMethodException e) {
      Method methodHadoop2;
      try {
        methodHadoop2 = SnappyCodec.class.getMethod("isNativeCodeLoaded", new Class[] {});
      } catch (NoSuchMethodException ex) {
        LOG.info("Can not determine if SnappyCodec is loaded.");
        return false;
      } catch (SecurityException ex) {
        LOG.error("Not allowed.", ex);
        return false;
      }
      Boolean loaded;
      try {
        loaded = (Boolean) methodHadoop2.invoke(null);
        if (loaded != null && loaded) {
          LOG.info("Using SnappyCodec");
          return true;
        } else {
          LOG.info("Not using SnappyCodec");
          return false;
        }
      } catch (Exception ex) {
        LOG.info("Unknown error while trying to determine if SnappyCodec is loaded.", ex);
        return false;
      }
    } catch (SecurityException e) {
      LOG.error("Not allowed.", e);
      return false;
    } catch (Exception e) {
      LOG.info("Unknown error while trying to determine if SnappyCodec is loaded.", e);
      return false;
    }
  }

  @Override
  public long getSegmentImportPendingCount() throws IOException {
    if (_indexImporter != null) {
      return _indexImporter.getSegmentImportPendingCount();
    }
    return 0l;
  }

  @Override
  public long getSegmentImportInProgressCount() throws IOException {
    if (_indexImporter != null) {
      return _indexImporter.getSegmentImportInProgressCount();
    }
    return 0l;
  }

  @Override
  public long getOnDiskSize() throws IOException {
    long total = 0;
    String[] listAll = _directory.listAll();
    for (String name : listAll) {
      try {
        total += _directory.fileLength(name);
      } catch (FileNotFoundException e) {
        // If file is not found that means that is was removed between the time
        // we started iterating over the file names and when we asked for it's
        // size.
      }
    }
    return total;
  }
}
