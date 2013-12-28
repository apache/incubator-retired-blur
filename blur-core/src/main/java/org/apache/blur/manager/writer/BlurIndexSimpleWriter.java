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
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.codec.Blur022Codec;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceCounter;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceFileGC;
import org.apache.blur.lucene.warmup.TraceableDirectory;
import org.apache.blur.manager.indexserver.BlurIndexWarmup;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.Row;
import org.apache.hadoop.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
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
  private final Thread _writerOpener;
  private final IndexWriterConfig _conf;
  private final TableContext _tableContext;
  private final FieldManager _fieldManager;
  private final BlurIndexRefresher _refresher;
  private final ShardContext _shardContext;
  private final AtomicReference<BlurIndexWriter> _writer = new AtomicReference<BlurIndexWriter>();
  private final boolean _makeReaderExitable = true;
  private IndexImporter _indexImporter;
  private final ReadWriteLock _lock = new ReentrantReadWriteLock();
  private final Lock _readLock = _lock.readLock();

  public BlurIndexSimpleWriter(ShardContext shardContext, Directory directory, SharedMergeScheduler mergeScheduler,
      DirectoryReferenceFileGC gc, final ExecutorService searchExecutor, BlurIndexCloser indexCloser,
      BlurIndexRefresher refresher, BlurIndexWarmup indexWarmup) throws IOException {
    super(shardContext, directory, mergeScheduler, gc, searchExecutor, indexCloser, refresher, indexWarmup);
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

    if (!DirectoryReader.indexExists(directory)) {
      new BlurIndexWriter(directory, _conf).close();
    }
    DirectoryReferenceCounter referenceCounter = new DirectoryReferenceCounter(directory, gc);
    // This directory allows for warm up by adding tracing ability.
    TraceableDirectory dir = new TraceableDirectory(referenceCounter);
    _directory = dir;

    // _directory = directory;

    _indexCloser = indexCloser;
    _indexReader.set(wrap(DirectoryReader.open(_directory)));
    _refresher = refresher;

    _writerOpener = getWriterOpener(shardContext);
    _writerOpener.start();
    _refresher.register(this);
  }

  private DirectoryReader wrap(DirectoryReader reader) {
    if (_makeReaderExitable) {
      reader = new ExitableReader(reader);
    }
    return reader;
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
          _indexImporter = new IndexImporter(_writer.get(), _lock, _shardContext, TimeUnit.SECONDS, 10);
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
    final IndexReader indexReader = _indexReader.get();
    while (!indexReader.tryIncRef()) {
      // keep trying to increment the ref
    }
    return new IndexSearcherClosable(indexReader, _searchThreadPool) {
      @Override
      public Directory getDirectory() {
        return _directory;
      }

      @Override
      public void close() throws IOException {
        indexReader.decRef();
      }
    };
  }

  @Override
  public void replaceRow(boolean waitToBeVisible, boolean wal, Row row) throws IOException {
    _readLock.lock();
    try {
      waitUntilNotNull(_writer);
      BlurIndexWriter writer = _writer.get();
      List<List<Field>> docs = TransactionRecorder.getDocs(row, _fieldManager);
      writer.updateDocuments(TransactionRecorder.createRowId(row.getId()), docs);
      waitToBeVisible(waitToBeVisible);
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public void deleteRow(boolean waitToBeVisible, boolean wal, String rowId) throws IOException {
    _readLock.lock();
    try {
      waitUntilNotNull(_writer);
      BlurIndexWriter writer = _writer.get();
      writer.deleteDocuments(TransactionRecorder.createRowId(rowId));
      waitToBeVisible(waitToBeVisible);
    } finally {
      _readLock.unlock();
    }
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
    IOUtils.cleanup(LOG, _indexImporter, _writer.get(), _indexReader.get());
  }

  @Override
  public void refresh() throws IOException {
    DirectoryReader currentReader = _indexReader.get();
    DirectoryReader newReader = DirectoryReader.openIfChanged(currentReader);
    if (newReader != null) {
      LOG.debug("Refreshing index for table [{0}] shard [{1}].", _tableContext.getTable(), _shardContext.getShard());
      _indexReader.set(wrap(newReader));
      _indexCloser.close(currentReader);
    }
  }

  @Override
  public AtomicBoolean isClosed() {
    return _isClosed;
  }

  @Override
  public void optimize(int numberOfSegmentsPerShard) throws IOException {
    throw new RuntimeException("not impl");
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

  private void waitToBeVisible(boolean waitToBeVisible) throws IOException {
    if (waitToBeVisible) {
      waitUntilNotNull(_writer);
      BlurIndexWriter writer = _writer.get();
      writer.commit();
      refresh();
    }
  }

}
