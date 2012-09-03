package com.nearinfinity.blur.manager.writer;

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
import static com.nearinfinity.blur.lucene.LuceneConstant.LUCENE_VERSION;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.analysis.BlurAnalyzer;

public abstract class AbstractBlurIndex extends BlurIndex {

  private BlurAnalyzer _analyzer;
  private BlurIndexCloser _closer;
  private Directory _directory;
  private IndexDeletionPolicy _indexDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
  private AtomicReference<IndexReader> _indexReaderRef = new AtomicReference<IndexReader>();
  private AtomicBoolean _isClosed = new AtomicBoolean(false);
  private AtomicBoolean _open = new AtomicBoolean();
  private BlurIndexRefresher _refresher;
  private String _shard;
  private Similarity _similarity;
  private String _table;

  protected IndexWriterConfig initIndexWriterConfig() {
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, _analyzer);
    conf.setWriteLockTimeout(TimeUnit.MINUTES.toMillis(5));
    conf.setIndexDeletionPolicy(_indexDeletionPolicy);
    conf.setSimilarity(_similarity);
    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    _open.set(true);
    return conf;
  }

  protected void initIndexReader(IndexReader reader) throws IOException {
    _indexReaderRef.set(reader);
    _refresher.register(this);
  }

  @Override
  public void refresh() throws IOException {
    if (!_open.get()) {
      return;
    }
    IndexReader oldReader = _indexReaderRef.get();
    if (oldReader.isCurrent()) {
      return;
    }
    IndexReader reader = IndexReader.openIfChanged(oldReader);
    if (reader != null && oldReader != reader) {
      _indexReaderRef.set(reader);
      _closer.close(oldReader);
    }
  }

  @Override
  public IndexReader getIndexReader() throws IOException {
    IndexReader indexReader = _indexReaderRef.get();
    indexReader.incRef();
    return indexReader;
  }

  @Override
  public void close() throws IOException {
    close(null);
    _directory.close();
  }

  public void close(Callable<Void> innerClose) throws IOException {
    _open.set(false);
    _refresher.unregister(this);
    if (innerClose != null) {
      try {
        innerClose.call();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    _isClosed.set(true);
  }

  @Override
  public AtomicBoolean isClosed() {
    return _isClosed;
  }

  public void setAnalyzer(BlurAnalyzer analyzer) {
    _analyzer = analyzer;
  }

  public void setCloser(BlurIndexCloser closer) {
    _closer = closer;
  }

  public void setDirectory(Directory directory) {
    _directory = directory;
  }

  public void setIndexDeletionPolicy(IndexDeletionPolicy indexDeletionPolicy) {
    _indexDeletionPolicy = indexDeletionPolicy;
  }

  public void setRefresher(BlurIndexRefresher refresher) {
    _refresher = refresher;
  }

  public void setShard(String shard) {
    this._shard = shard;
  }

  public void setSimilarity(Similarity similarity) {
    _similarity = similarity;
  }

  public void setTable(String table) {
    this._table = table;
  }

  protected BlurAnalyzer getAnalyzer() {
    return _analyzer;
  }

  protected Directory getDirectory() {
    return _directory;
  }

  protected String getShard() {
    return _shard;
  }

  protected String getTable() {
    return _table;
  }

  protected boolean isOpen() {
    return _open.get();
  }
}
