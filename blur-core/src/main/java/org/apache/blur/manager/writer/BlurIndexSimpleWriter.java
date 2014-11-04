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
package org.apache.blur.manager.writer;

import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_QUEUE_MAX_INMEMORY_LENGTH;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.BlurIndexWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;

public class BlurIndexSimpleWriter extends BlurIndex {

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
  private IndexImporter _indexImporter;
  private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
  private final WriteLock _writeLock = _lock.writeLock();
  private final ReadWriteLock _indexRefreshLock = new ReentrantReadWriteLock();
  private final Lock _indexRefreshWriteLock = _indexRefreshLock.writeLock();
  private final Lock _indexRefreshReadLock = _indexRefreshLock.readLock();
  private Thread _optimizeThread;
  private Thread _writerOpener;
  private final IndexDeletionPolicyReader _policy;
  private final SnapshotIndexDeletionPolicy _snapshotIndexDeletionPolicy;
  private final String _context;
  private final AtomicInteger _writesWaiting = new AtomicInteger();
  private final BlockingQueue<RowMutation> _queue;
  private final MutationQueueProcessor _mutationQueueProcessor;
  private final Timer _indexImporterTimer;

  public BlurIndexSimpleWriter(ShardContext shardContext, Directory directory, SharedMergeScheduler mergeScheduler,
      final ExecutorService searchExecutor, BlurIndexCloser indexCloser, Timer indexImporterTimer) throws IOException {
    super(shardContext, directory, mergeScheduler, searchExecutor, indexCloser, indexImporterTimer);
    _indexImporterTimer = indexImporterTimer;
    _searchThreadPool = searchExecutor;
    _shardContext = shardContext;
    _tableContext = _shardContext.getTableContext();
    _context = _tableContext.getTable() + "/" + shardContext.getShard();
    _fieldManager = _tableContext.getFieldManager();
    Analyzer analyzer = _fieldManager.getAnalyzerForIndex();
    _conf = new IndexWriterConfig(LUCENE_VERSION, analyzer);
    _conf.setWriteLockTimeout(TimeUnit.MINUTES.toMillis(5));
    _conf.setCodec(new Blur024Codec(_tableContext.getBlurConfiguration()));
    _conf.setSimilarity(_tableContext.getSimilarity());
    _conf.setInfoStream(new LoggingInfoStream(_tableContext.getTable(),_shardContext.getShard()));
    TieredMergePolicy mergePolicy = (TieredMergePolicy) _conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    _conf.setMergeScheduler(mergeScheduler.getMergeScheduler());
    _snapshotIndexDeletionPolicy = new SnapshotIndexDeletionPolicy(_tableContext.getConfiguration(), new Path(
        shardContext.getHdfsDirPath(), "generations"));
    _policy = new IndexDeletionPolicyReader(_snapshotIndexDeletionPolicy);
    _conf.setIndexDeletionPolicy(_policy);
    BlurConfiguration blurConfiguration = _tableContext.getBlurConfiguration();
    _queue = new ArrayBlockingQueue<RowMutation>(blurConfiguration.getInt(BLUR_SHARD_QUEUE_MAX_INMEMORY_LENGTH, 100));
    _mutationQueueProcessor = new MutationQueueProcessor(_queue, this, _shardContext, _writesWaiting);

    if (!DirectoryReader.indexExists(directory)) {
      new BlurIndexWriter(directory, _conf).close();
    }

    _directory = directory;

    _indexCloser = indexCloser;
    _indexReader.set(wrap(DirectoryReader.open(_directory)));

    openWriter();
  }

  private synchronized void openWriter() {
    IOUtils.cleanup(LOG, _indexImporter);
    BlurIndexWriter writer = _writer.get();
    if (writer != null) {
      try {
        writer.close(false);
      } catch (IOException e) {
        LOG.error("Unknown error while trying to close the writer, [" + _shardContext.getTableContext().getTable()
            + "] Shard [" + _shardContext.getShard() + "]", e);
      }
      _writer.set(null);
    }
    _writerOpener = getWriterOpener(_shardContext);
    _writerOpener.start();
  }

  private DirectoryReader wrap(DirectoryReader reader) throws IOException {
    if (_makeReaderExitable) {
      reader = new ExitableReader(reader);
    }
    return _policy.register(reader);
  }

  private Thread getWriterOpener(ShardContext shardContext) {
    Thread thread = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          _writer.set(new BlurIndexWriter(_directory, _conf.clone()));
          synchronized (_writer) {
            _writer.notify();
          }
          _indexImporter = new IndexImporter(_indexImporterTimer, BlurIndexSimpleWriter.this, _shardContext,
              TimeUnit.SECONDS, 10);
        } catch (IOException e) {
          LOG.error("Unknown error on index writer open.", e);
        }
      }
    });
    thread.setName("Writer Opener for Table [" + shardContext.getTableContext().getTable() + "] Shard ["
        + shardContext.getShard() + "]");
    thread.setDaemon(true);
    return thread;
  }

  @Override
  public IndexSearcherClosable getIndexSearcher() throws IOException {
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
    return new IndexSearcherClosable(indexReader, _searchThreadPool) {

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

  private void waitUntilNotNull(AtomicReference<?> ref) {
    while (true) {
      Object object = ref.get();
      if (object != null) {
        return;
      }
      synchronized (ref) {
        try {
          ref.wait(TimeUnit.SECONDS.toMillis(1));
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    _isClosed.set(true);
    IOUtils.cleanup(LOG, _indexImporter, _mutationQueueProcessor, _writer.get(), _indexReader.get());
  }

  @Override
  public void refresh() throws IOException {

  }

  @Override
  public AtomicBoolean isClosed() {
    return _isClosed;
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
          waitUntilNotNull(_writer);
          BlurIndexWriter writer = _writer.get();
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
    waitUntilNotNull(_writer);
    BlurIndexWriter writer = _writer.get();
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
    waitUntilNotNull(_writer);
    BlurIndexWriter writer = _writer.get();
    IndexSearcherClosable indexSearcher = null;
    try {
      indexSearcher = getIndexSearcher();
      indexAction.performMutate(indexSearcher, writer);
      indexAction.doPreCommit(indexSearcher, writer);
      commit();
      indexAction.doPostCommit(writer);
    } catch (Exception e) {
      indexAction.doPreRollback(writer);
      writer.rollback();
      openWriter();
      indexAction.doPostRollback(writer);
      throw new IOException("Unknown error during mutation", e);
    } finally {
      if (indexSearcher != null) {
        indexSearcher.close();
      }
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

}
