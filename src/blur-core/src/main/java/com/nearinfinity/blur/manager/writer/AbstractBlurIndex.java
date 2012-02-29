package com.nearinfinity.blur.manager.writer;

import static com.nearinfinity.blur.lucene.LuceneConstant.LUCENE_VERSION;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.index.DirectIODirectory;
import com.nearinfinity.blur.manager.clusterstatus.ClusterStatus;
import com.nearinfinity.blur.utils.BlurConstants;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.Similarity;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractBlurIndex extends BlurIndex {

  private BlurAnalyzer _analyzer;
  private BlurIndexCloser _closer;
  private ClusterStatus _clusterStatus;
  private DirectIODirectory _directory;
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
    _indexReaderRef.set(updateSchema(reader));
    _refresher.register(this);
  }

  private IndexReader updateSchema(IndexReader reader) {
    if (_clusterStatus != null) {
      Collection<String> fieldNames = reader.getFieldNames(FieldOption.ALL);
      _clusterStatus.writeCacheFieldsForTable(BlurConstants.BLUR_CLUSTER,_table,fieldNames);
    }
    return reader;
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
    IndexReader reader = IndexReader.openIfChanged(oldReader, true);
    if (reader != null && oldReader != reader) {
      _indexReaderRef.set(updateSchema(reader));
      _closer.close(oldReader);
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
    close(null);
    _directory.close();
  }

  public void close(Callable<Void> innerClose) throws IOException {
    _open.set(false);
    _refresher.unregister(this);
    if (innerClose != null) {
      try {
        innerClose.call();
      } catch(Exception e) {
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

  public void setClusterStatus(ClusterStatus clusterStatus) {
    _clusterStatus = clusterStatus;
  }

  public void setDirectory(DirectIODirectory directory) {
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

  protected DirectIODirectory getDirectory() {
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
