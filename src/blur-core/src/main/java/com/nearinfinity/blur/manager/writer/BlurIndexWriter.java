/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.manager.writer;

import static com.nearinfinity.blur.utils.BlurConstants.ROW_ID;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.index.WalIndexWriter;
import com.nearinfinity.blur.index.WalIndexWriter.WalInputFactory;
import com.nearinfinity.blur.index.WalIndexWriter.WalOutputFactory;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.metrics.BlurMetrics;
import com.nearinfinity.blur.store.DirectIODirectory;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.RowWalIndexWriter;

public class BlurIndexWriter extends BlurIndex {

  private static final Log LOG = LogFactory.getLog(BlurIndexWriter.class);

  private DirectIODirectory _directory;
  private WalIndexWriter _writer;
  private BlurAnalyzer _analyzer;
  private AtomicReference<IndexReader> _indexReaderRef = new AtomicReference<IndexReader>();
  private BlurIndexCloser _closer;
  private BlurIndexRefresher _refresher;
  private RowWalIndexWriter _rowIndexWriter;
  private BlurIndexCommiter _commiter;
  private AtomicBoolean _open = new AtomicBoolean();
  private String _id = UUID.randomUUID().toString();
  private BlurMetrics _blurMetrics;

  public void init() throws IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_34, _analyzer);
    conf.setSimilarity(new FairSimilarity());
    conf.setWriteLockTimeout(TimeUnit.MINUTES.toMillis(5));
    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    _writer = new WalIndexWriter(_directory, conf, _blurMetrics, new WalOutputFactory() {
      @Override
      public IndexOutput getWalOutput(DirectIODirectory directory, String name) throws IOException {
        return directory.createOutputDirectIO(name);
      }
    }, new WalInputFactory() {
      @Override
      public IndexInput getWalInput(DirectIODirectory directory, String name) throws IOException {
        return directory.openInputDirectIO(name);
      }
    });
    _writer.commitAndRollWal();
    _indexReaderRef.set(IndexReader.open(_writer, true));
    _rowIndexWriter = new RowWalIndexWriter(_writer, _analyzer);
    _open.set(true);
    _refresher.register(this);
    _commiter.addWriter(_id, _writer);
  }

  @Override
  public void refresh() throws IOException {
    synchronized (_writer) {
      if (!_open.get()) {
        return;
      }
      IndexReader oldReader = _indexReaderRef.get();
      if (oldReader.isCurrent()) {
        return;
      }
      try {
        IndexReader reader = oldReader.reopen(_writer, true);
        if (oldReader != reader) {
          _indexReaderRef.set(reader);
          _closer.close(oldReader);
        }
      } catch (AlreadyClosedException e) {
        LOG.warn("Writer was already closed, this can happen during closing of a writer.");
      }
    }
  }

  @Override
  public IndexReader getIndexReader(boolean forceRefresh) throws IOException {
    if (forceRefresh) {
      refresh();
    }
    IndexReader indexReader = _indexReaderRef.get();
    indexReader.incRef();
    return indexReader;
  }

  @Override
  public void close() throws IOException {
    _commiter.remove(_id);
    _open.set(false);
    _refresher.unregister(this);
    _writer.close();
  }

  @Override
  public boolean replaceRow(boolean wal, Row row) throws IOException {
    synchronized (_writer) {
      _rowIndexWriter.replace(wal, row);
      return true;
    }
  }

  @Override
  public void deleteRow(boolean wal, String rowId) throws IOException {
    synchronized (_writer) {
      _writer.deleteDocuments(wal, new Term(ROW_ID, rowId));
    }
  }

  public void setAnalyzer(BlurAnalyzer analyzer) {
    _analyzer = analyzer;
  }

  public void setDirectory(DirectIODirectory directory) {
    _directory = directory;
  }

  public void setCloser(BlurIndexCloser closer) {
    _closer = closer;
  }

  public void setRefresher(BlurIndexRefresher refresher) {
    _refresher = refresher;
  }

  public void setCommiter(BlurIndexCommiter commiter) {
    _commiter = commiter;
  }

  public void setBlurMetrics(BlurMetrics blurMetrics) {
    _blurMetrics = blurMetrics;
  }
  
  
}
