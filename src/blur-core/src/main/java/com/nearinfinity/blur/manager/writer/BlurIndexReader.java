package com.nearinfinity.blur.manager.writer;

import static com.nearinfinity.blur.lucene.LuceneConstant.LUCENE_VERSION;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.search.Similarity;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.index.DirectIODirectory;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.clusterstatus.ClusterStatus;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurIndexReader extends BlurIndex {

  private static final Log LOG = LogFactory.getLog(BlurIndexReader.class);

  private DirectIODirectory _directory;
  private BlurAnalyzer _analyzer;
  private AtomicReference<IndexReader> _indexReaderRef = new AtomicReference<IndexReader>();
  private BlurIndexCloser _closer;
  private BlurIndexRefresher _refresher;
  private AtomicBoolean _open = new AtomicBoolean();
  private String _table;
  private String _shard;
  private AtomicBoolean _isClosed = new AtomicBoolean(false);
  private IndexDeletionPolicy _indexDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
  private Similarity _similarity;
  private ClusterStatus _clusterStatus;

  public void init() throws IOException {
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, _analyzer);
    conf.setWriteLockTimeout(TimeUnit.MINUTES.toMillis(5));
    conf.setIndexDeletionPolicy(_indexDeletionPolicy);
    conf.setSimilarity(_similarity);
    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    _open.set(true);
    _indexReaderRef.set(updateSchema(IndexReader.open(_directory, true)));
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
  public synchronized void refresh() throws IOException {
    if (!_open.get()) {
      return;
    }
    IndexReader oldReader = _indexReaderRef.get();
    if (oldReader.isCurrent()) {
      LOG.debug("Old reader is current.");
      return;
    }
    IndexReader reader = IndexReader.openIfChanged(oldReader, true);
    if (reader != null && oldReader != reader) {
      LOG.debug("Setting up new reader.");
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
    _open.set(false);
    _refresher.unregister(this);
    _isClosed.set(true);
    LOG.info("Reader for table [{0}] shard [{1}] closed.",_table,_shard);
  }

  @Override
  public boolean replaceRow(boolean wal, Row row) throws IOException {
    throw new RuntimeException("Read-only shard");
  }

  @Override
  public void deleteRow(boolean wal, String rowId) throws IOException {
    throw new RuntimeException("Read-only shard");
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

  public void setTable(String table) {
    this._table = table;
  }

  public void setShard(String shard) {
    this._shard = shard;
  }

  @Override
  public AtomicBoolean isClosed() {
    return _isClosed;
  }

  public void setIndexDeletionPolicy(IndexDeletionPolicy indexDeletionPolicy) {
    _indexDeletionPolicy = indexDeletionPolicy;
  }

  public void setSimilarity(Similarity similarity) {
    _similarity = similarity;
  }

  public void setClusterStatus(ClusterStatus clusterStatus) {
    _clusterStatus = clusterStatus;
  }

  @Override
  public void optimize(int numberOfSegmentsPerShard) {
    
  }
}
