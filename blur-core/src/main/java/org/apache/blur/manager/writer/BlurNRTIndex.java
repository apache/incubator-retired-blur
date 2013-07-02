package org.apache.blur.manager.writer;

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
import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.index.ExitableReader;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceCounter;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceFileGC;
import org.apache.blur.lucene.store.refcounter.IndexInputCloser;
import org.apache.blur.lucene.warmup.TraceableDirectory;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.IndexSearcherClosableNRT;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.BlurIndexWriter;
import org.apache.lucene.index.BlurIndexWriter.LockOwnerException;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.search.NRTManager.TrackingIndexWriter;
import org.apache.lucene.search.NRTManagerReopenThread;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;

public class BlurNRTIndex extends BlurIndex {

  private static final Log LOG = LogFactory.getLog(BlurNRTIndex.class);
  private static final boolean APPLY_ALL_DELETES = true;

  private final AtomicReference<NRTManager> _nrtManagerRef = new AtomicReference<NRTManager>();
  private final AtomicBoolean _isClosed = new AtomicBoolean();
  private final BlurIndexWriter _writer;
  private final Thread _committer;
  private final SearcherFactory _searcherFactory;
  private final Directory _directory;
  private final NRTManagerReopenThread _refresher;
  private final TableContext _tableContext;
  private final ShardContext _shardContext;
  private final TransactionRecorder _recorder;
  private final TrackingIndexWriter _trackingWriter;
  // This lock is used during a import of data from the file system. For example
  // after a mapreduce program.
  private final ReadWriteLock _lock = new ReentrantReadWriteLock();
  private long _lastRefresh = 0;
  private IndexImporter _indexImporter;

  public BlurNRTIndex(ShardContext shardContext, SharedMergeScheduler mergeScheduler, IndexInputCloser closer,
      Directory directory, DirectoryReferenceFileGC gc, final ExecutorService searchExecutor) throws IOException {
    _tableContext = shardContext.getTableContext();
    _directory = directory;
    _shardContext = shardContext;

    FieldManager fieldManager = _tableContext.getFieldManager();
    Analyzer analyzer = fieldManager.getAnalyzerForIndex();
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, analyzer);
    conf.setWriteLockTimeout(TimeUnit.MINUTES.toMillis(5));
    conf.setSimilarity(_tableContext.getSimilarity());
    conf.setIndexDeletionPolicy(_tableContext.getIndexDeletionPolicy());
    conf.setMergedSegmentWarmer(new FieldBasedWarmer(shardContext, _isClosed));

    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    conf.setMergeScheduler(mergeScheduler);

    DirectoryReferenceCounter referenceCounter = new DirectoryReferenceCounter(directory, gc, closer);
    // This directory allows for warm up by adding tracing ability.
    TraceableDirectory dir = new TraceableDirectory(referenceCounter);
    _writer = new BlurIndexWriter(dir, conf, true);
    _recorder = new TransactionRecorder(shardContext);
    _recorder.replay(_writer);

    _searcherFactory = new SearcherFactory() {
      @Override
      public IndexSearcher newSearcher(IndexReader reader) throws IOException {
        return new IndexSearcherClosableNRT(reader, searchExecutor, _nrtManagerRef, _directory);
      }
    };

    _trackingWriter = new TrackingIndexWriter(_writer);
    _indexImporter = new IndexImporter(_trackingWriter, _lock, _shardContext, TimeUnit.SECONDS, 10);
    _nrtManagerRef.set(new NRTManager(_trackingWriter, _searcherFactory, APPLY_ALL_DELETES));
    // start commiter

    _committer = new Thread(new Committer());
    _committer.setDaemon(true);
    _committer.setName("Commit Thread [" + _tableContext.getTable() + "/" + shardContext.getShard() + "]");
    _committer.start();

    // start refresher
    double targetMinStaleSec = _tableContext.getTimeBetweenRefreshs() / 1000.0;
    _refresher = new NRTManagerReopenThread(getNRTManager(), targetMinStaleSec * 10, targetMinStaleSec);
    _refresher.setName("Refresh Thread [" + _tableContext.getTable() + "/" + shardContext.getShard() + "]");
    _refresher.setDaemon(true);
    _refresher.start();
  }

  @Override
  public void replaceRow(boolean waitToBeVisible, boolean wal, Row row) throws IOException {
    _lock.readLock().lock();
    try {
      List<Record> records = row.records;
      if (records == null || records.isEmpty()) {
        deleteRow(waitToBeVisible, wal, row.id);
        return;
      }
      long generation = _recorder.replaceRow(wal, row, _trackingWriter);
      waitToBeVisible(waitToBeVisible, generation);
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public void deleteRow(boolean waitToBeVisible, boolean wal, String rowId) throws IOException {
    _lock.readLock().lock();
    try {
      long generation = _recorder.deleteRow(wal, rowId, _trackingWriter);
      waitToBeVisible(waitToBeVisible, generation);
    } finally {
      _lock.readLock().unlock();
    }
  }

  /**
   * The method fetches a reference to the IndexSearcher, the caller is
   * responsible for calling close on the searcher.
   */
  @Override
  public IndexSearcherClosable getIndexReader() throws IOException {
    return resetRunning((IndexSearcherClosable) getNRTManager().acquire());
  }

  private IndexSearcherClosable resetRunning(IndexSearcherClosable indexSearcherClosable) {
    IndexReader indexReader = indexSearcherClosable.getIndexReader();
    if (indexReader instanceof ExitableReader) {
      ExitableReader er = (ExitableReader) indexReader;
      er.getRunning().set(true);
    }
    return indexSearcherClosable;
  }

  private NRTManager getNRTManager() {
    return _nrtManagerRef.get();
  }

  @Override
  public void close() throws IOException {
    // @TODO make sure that locks are cleaned up.
    if (!_isClosed.get()) {
      _isClosed.set(true);
      _indexImporter.close();
      _committer.interrupt();
      _refresher.close();
      try {
        _recorder.close();
        _writer.close(false);
        getNRTManager().close();
      } finally {
        _directory.close();
      }
    }
  }

  @Override
  public void refresh() throws IOException {
    getNRTManager().maybeRefresh();
    _lastRefresh = System.currentTimeMillis();
  }

  @Override
  public AtomicBoolean isClosed() {
    return _isClosed;
  }

  @Override
  public void optimize(int numberOfSegmentsPerShard) throws IOException {
    _writer.forceMerge(numberOfSegmentsPerShard);
  }

  private void waitToBeVisible(boolean waitToBeVisible, long generation) throws IOException {
    if (needsRefresh()) {
      refresh();
    }
    if (waitToBeVisible && getNRTManager().getCurrentSearchingGen() < generation) {
      getNRTManager().waitForGeneration(generation);
    }
  }

  private boolean needsRefresh() {
    if (_lastRefresh + _tableContext.getTimeBetweenRefreshs() < System.currentTimeMillis()) {
      return true;
    }
    return false;
  }

  class Committer implements Runnable {
    @Override
    public void run() {
      synchronized (this) {
        while (!_isClosed.get()) {
          try {
            LOG.debug("Committing of [{0}/{1}].", _tableContext.getTable(), _shardContext.getShard());
            _recorder.commit(_writer);
          } catch (CorruptIndexException e) {
            LOG.error("Curruption Error during commit of [{0}/{1}].", e, _tableContext.getTable(),
                _shardContext.getShard());
          } catch (LockOwnerException e) {
            LOG.info("This shard server no longer owns the lock on [{0}/{1}], closing.", _tableContext.getTable(),
                _shardContext.getShard());
            try {
              close();
            } catch (IOException ex) {
              LOG.error("Unknown error while trying to close [{0}/{1}]", _tableContext.getTable(),
                  _shardContext.getShard());
            }
            return;
          } catch (IOException e) {
            LOG.error("IO Error during commit of [{0}/{1}].", e, _tableContext.getTable(), _shardContext.getShard());
          }
          try {
            wait(_tableContext.getTimeBetweenCommits());
          } catch (InterruptedException e) {
            if (_isClosed.get()) {
              return;
            }
            LOG.error("Unknown error with committer thread [{0}/{1}].", e, _tableContext.getTable(),
                _shardContext.getShard());
          }
        }
      }
    }
  }
}