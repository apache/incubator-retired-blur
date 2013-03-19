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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.index.IndexWriter;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceCounter;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceFileGC;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.codecs.appending.AppendingCodec;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.search.NRTManager.TrackingIndexWriter;
import org.apache.lucene.search.NRTManagerReopenThread;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;

public class BlurNRTIndex extends BlurIndex {

  private static final Log LOG = LogFactory.getLog(BlurNRTIndex.class);
  private static final boolean APPLY_ALL_DELETES = true;

  private NRTManager _nrtManager;
  private AtomicBoolean _isClosed = new AtomicBoolean();
  private IndexWriter _writer;
  private Thread _committer;

  // externally set
  private BlurAnalyzer _analyzer;
  private Directory _directory;
  private String _table;
  private String _shard;
  private Similarity _similarity;
  private NRTManagerReopenThread _refresher;
  private TransactionRecorder _recorder;
  private Configuration _configuration;
  private Path _walPath;
  private IndexDeletionPolicy _indexDeletionPolicy;
  private BlurIndexCloser _closer;
  private AtomicReference<IndexReader> _indexRef = new AtomicReference<IndexReader>();
  private long _timeBetweenCommits = TimeUnit.SECONDS.toMillis(60);
  private long _timeBetweenRefreshs = TimeUnit.MILLISECONDS.toMillis(5000);
  private DirectoryReferenceFileGC _gc;
  private TrackingIndexWriter _trackingWriter;
  private SearcherFactory _searcherFactory = new SearcherFactory();
  private long _lastRefresh;
  private long _timeBetweenRefreshsNanos;

  // private SearcherWarmer _warmer = new SearcherWarmer() {
  // @Override
  // public void warm(IndexSearcher s) throws IOException {
  // IndexReader indexReader = s.getIndexReader();
  // IndexReader[] subReaders = indexReader.getSequentialSubReaders();
  // if (subReaders == null) {
  // PrimeDocCache.getPrimeDocBitSet(indexReader);
  // } else {
  // for (IndexReader reader : subReaders) {
  // PrimeDocCache.getPrimeDocBitSet(reader);
  // }
  // }
  // }
  // };

  public void init() throws IOException {
    tableContext = shardContext.getTableContext();

    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, tableContext.getAnalyzer());
    conf.setWriteLockTimeout(TimeUnit.MINUTES.toMillis(5));
    conf.setSimilarity(tableContext.getSimilarity());
    conf.setIndexDeletionPolicy(tableContext.getIndexDeletionPolicy());
    // conf.setCodec(new AppendingCodec());
    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    conf.setMergeScheduler(mergeScheduler);
    DirectoryReferenceCounter referenceCounter = new DirectoryReferenceCounter(_directory, _gc, _closer);
    _writer = new IndexWriter(referenceCounter, conf);
    _recorder = new TransactionRecorder();
    _recorder.setContext(shardContext);
    _recorder.init();
    _recorder.replay(_writer);

    _searcherFactory = new SearcherFactory() {
      @Override
      public IndexSearcher newSearcher(IndexReader reader) throws IOException {
        return new IndexSearcherClosable(reader, searchExecutor, _nrtManagerRef);
      }
    };

    _trackingWriter = new TrackingIndexWriter(_writer);
    _nrtManagerRef.set(new NRTManager(_trackingWriter, _searcherFactory, APPLY_ALL_DELETES));
    startCommiter();
    startRefresher();
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

  @Override
  public IndexReader getIndexReader() throws IOException {
    IndexReader indexReader = _indexRef.get();
    while (!indexReader.tryIncRef()) {
      indexReader = _indexRef.get();
    }
    LOG.debug("Index fetched with ref of [{0}] [{1}]", indexReader.getRefCount(), indexReader);
    return indexReader;
  }

  @Override
  public void close() throws IOException {
    // @TODO make sure that locks are cleaned up.
    _isClosed.set(true);
    _committer.interrupt();
    _refresher.close();
    try {
      _recorder.close();
      _writer.close();
      _closer.close(_indexRef.get());
      _nrtManager.close();
    } finally {
      _directory.close();
    }
  }

  @Override
  public void refresh() throws IOException {
    _nrtManager.maybeRefresh();
    swap();
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
    if (waitToBeVisible && _nrtManager.getCurrentSearchingGen() < generation) {
      // if visibility is required then reopen.
      _nrtManager.waitForGeneration(generation);
      swap();
    } else {
      long now = System.nanoTime();
      if (_lastRefresh + _timeBetweenRefreshsNanos < now) {
        refresh();
        _lastRefresh = now;
      }
    }
  }

  private void swap() {
    IndexSearcher searcher = _nrtManager.acquire();
    IndexReader indexReader = searcher.getIndexReader();
    IndexReader oldIndexReader = _indexRef.getAndSet(indexReader);
    _closer.close(oldIndexReader);
  }

  public void setAnalyzer(BlurAnalyzer analyzer) {
    _analyzer = analyzer;
  }

  public void setDirectory(Directory directory) {
    _directory = directory;
  }

  public void setTable(String table) {
    _table = table;
  }

  public void setShard(String shard) {
    _shard = shard;
  }

  public void setSimilarity(Similarity similarity) {
    _similarity = similarity;
  }

  public void setTimeBetweenCommits(long timeBetweenCommits) {
    _timeBetweenCommits = timeBetweenCommits;
  }

  public void setTimeBetweenRefreshs(long timeBetweenRefreshs) {
    _timeBetweenRefreshs = timeBetweenRefreshs;
  }

  public void setWalPath(Path walPath) {
    _walPath = walPath;
  }

  public void setConfiguration(Configuration configuration) {
    _configuration = configuration;
  }

  public void setIndexDeletionPolicy(IndexDeletionPolicy indexDeletionPolicy) {
    _indexDeletionPolicy = indexDeletionPolicy;
  }

  public void setCloser(BlurIndexCloser closer) {
    _closer = closer;
  }

  public DirectoryReferenceFileGC getGc() {
    return _gc;
  }

  public void setGc(DirectoryReferenceFileGC gc) {
    _gc = gc;
  }

  private void startRefresher() {
    double targetMinStaleSec = _timeBetweenRefreshs / 1000.0;
    _refresher = new NRTManagerReopenThread(_nrtManager, targetMinStaleSec * 10, targetMinStaleSec);
    _refresher.setName("Refresh Thread [" + _table + "/" + _shard + "]");
    _refresher.setDaemon(true);
    _refresher.start();
  }

  private void startCommiter() {
    _committer = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!_isClosed.get()) {
          try {
            LOG.info("Committing of [{0}/{1}].", _table, _shard);
            _recorder.commit(_writer);
          } catch (CorruptIndexException e) {
            LOG.error("Curruption Error during commit of [{0}/{1}].", e, _table, _shard);
          } catch (IOException e) {
            LOG.error("IO Error during commit of [{0}/{1}].", e, _table, _shard);
          }
          try {
            Thread.sleep(_timeBetweenCommits);
          } catch (InterruptedException e) {
            if (_isClosed.get()) {
              return;
            }
            LOG.error("Unknown error with committer thread [{0}/{1}].", e, _table, _shard);
          }
        }
      }
    });
    _committer.setDaemon(true);
    _committer.setName("Commit Thread [" + _table + "/" + _shard + "]");
    _committer.start();
  }

}