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

import org.apache.blur.index.IndexWriter;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceCounter;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceFileGC;
import org.apache.blur.lucene.store.refcounter.IndexInputCloser;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
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
  private final IndexWriter _writer;
  private final Thread _committer;
  private final SearcherFactory _searcherFactory;
  private final Directory _directory;
  private final NRTManagerReopenThread _refresher;
  private final TableContext _tableContext;
  private final ShardContext _shardContext;
  private final TransactionRecorder _recorder;
  private final TrackingIndexWriter _trackingWriter;

  private long _lastRefresh = 0;

  public BlurNRTIndex(ShardContext shardContext, SharedMergeScheduler mergeScheduler, IndexInputCloser closer,
      Directory directory, DirectoryReferenceFileGC gc, final ExecutorService searchExecutor) throws IOException {
    _tableContext = shardContext.getTableContext();
    _directory = directory;
    _shardContext = shardContext;

    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, _tableContext.getAnalyzer());
    conf.setWriteLockTimeout(TimeUnit.MINUTES.toMillis(5));
    conf.setSimilarity(_tableContext.getSimilarity());
    conf.setIndexDeletionPolicy(_tableContext.getIndexDeletionPolicy());
    conf.setMergedSegmentWarmer(new FieldBasedWarmer(shardContext));

    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    conf.setMergeScheduler(mergeScheduler);

    DirectoryReferenceCounter referenceCounter = new DirectoryReferenceCounter(directory, gc, closer);

    _writer = new IndexWriter(referenceCounter, conf);
    _recorder = new TransactionRecorder(shardContext);
    _recorder.replay(_writer);

    _searcherFactory = new SearcherFactory() {
      @Override
      public IndexSearcher newSearcher(IndexReader reader) throws IOException {
        return new IndexSearcherClosable(reader, searchExecutor, _nrtManagerRef, _directory);
      }
    };

    _trackingWriter = new TrackingIndexWriter(_writer);
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
    List<Record> records = row.records;
    if (records == null || records.isEmpty()) {
      deleteRow(waitToBeVisible, wal, row.id);
      return;
    }
    long generation = _recorder.replaceRow(wal, row, _trackingWriter);
    waitToBeVisible(waitToBeVisible, generation);
  }

  @Override
  public void deleteRow(boolean waitToBeVisible, boolean wal, String rowId) throws IOException {
    long generation = _recorder.deleteRow(wal, rowId, _trackingWriter);
    waitToBeVisible(waitToBeVisible, generation);
  }

  /**
   * The method fetches a reference to the IndexSearcher, the caller is
   * responsible for calling close on the searcher.
   */
  @Override
  public IndexSearcherClosable getIndexReader() throws IOException {
    return (IndexSearcherClosable) getNRTManager().acquire();
  }

  private NRTManager getNRTManager() {
    return _nrtManagerRef.get();
  }

  @Override
  public void close() throws IOException {
    // @TODO make sure that locks are cleaned up.
    if (!_isClosed.get()) {
      _isClosed.set(true);
      _committer.interrupt();
      _refresher.close();
      try {
        _recorder.close();
        _writer.close();
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