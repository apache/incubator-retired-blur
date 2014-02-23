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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.index.ExitableReader;
import org.apache.blur.index.IndexDeletionPolicyReader;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.codec.Blur022Codec;
import org.apache.blur.lucene.warmup.TraceableDirectory;
import org.apache.blur.manager.indexserver.BlurIndexWarmup;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.hadoop.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.BlurIndexWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
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
  private QueueReader _queueReader;
  private final ReadWriteLock _lock = new ReentrantReadWriteLock();
  private final Lock _writeLock = _lock.writeLock();
  private final ReadWriteLock _indexRefreshLock = new ReentrantReadWriteLock();
  private final Lock _indexRefreshWriteLock = _indexRefreshLock.writeLock();
  private final Lock _indexRefreshReadLock = _indexRefreshLock.readLock();
  private Thread _optimizeThread;
  private Thread _writerOpener;
  private final IndexDeletionPolicyReader _policy;

  public BlurIndexSimpleWriter(ShardContext shardContext, Directory directory, SharedMergeScheduler mergeScheduler,
      final ExecutorService searchExecutor, BlurIndexCloser indexCloser, BlurIndexWarmup indexWarmup)
      throws IOException {
    super(shardContext, directory, mergeScheduler, searchExecutor, indexCloser, indexWarmup);
    _searchThreadPool = searchExecutor;
    _shardContext = shardContext;
    _tableContext = _shardContext.getTableContext();
    _fieldManager = _tableContext.getFieldManager();
    Analyzer analyzer = _fieldManager.getAnalyzerForIndex();
    _conf = new IndexWriterConfig(LUCENE_VERSION, analyzer);
    _conf.setWriteLockTimeout(TimeUnit.MINUTES.toMillis(5));
    _conf.setCodec(new Blur022Codec(_tableContext.getBlurConfiguration()));
    _conf.setSimilarity(_tableContext.getSimilarity());
    _conf.setMergedSegmentWarmer(new BlurIndexReaderWarmer(shardContext, _isClosed, indexWarmup));
    TieredMergePolicy mergePolicy = (TieredMergePolicy) _conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    _conf.setMergeScheduler(mergeScheduler.getMergeScheduler());
    _policy = new IndexDeletionPolicyReader(new KeepOnlyLastCommitDeletionPolicy());
    _conf.setIndexDeletionPolicy(_policy);

    if (!DirectoryReader.indexExists(directory)) {
      new BlurIndexWriter(directory, _conf).close();
    }

    // This directory allows for warm up by adding tracing ability.
    TraceableDirectory dir = new TraceableDirectory(directory);
    _directory = dir;

    _indexCloser = indexCloser;
    _indexReader.set(wrap(DirectoryReader.open(_directory)));

    openWriter();
  }

  private synchronized void openWriter() {
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
          _indexImporter = new IndexImporter(BlurIndexSimpleWriter.this, _shardContext, TimeUnit.SECONDS, 10);
          _queueReader = _tableContext.getQueueReader(BlurIndexSimpleWriter.this, _shardContext);
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
          LOG.error("Searcher already closed [{0}].", new Throwable(), this);
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
    IOUtils.cleanup(LOG, _indexImporter, _queueReader, _writer.get(), _indexReader.get());
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
    if (_optimizeThread == null || _optimizeThread.isAlive()) {
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
    throw new RuntimeException("not impl");
  }

  @Override
  public void removeSnapshot(String name) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public List<String> getSnapshots() throws IOException {
    throw new RuntimeException("not impl");
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
      LOG.error("Reader should be new after commit for table [{0}] shard [{1}].", _tableContext.getTable(),
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
    _writeLock.lock();
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

}
