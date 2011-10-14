package com.nearinfinity.blur.manager.writer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.RowIndexWriter;

public class BlurSimpleIndexWriter extends BlurIndex {
  
  private static final Log LOG = LogFactory.getLog(BlurSimpleIndexWriter.class);
  
  private Directory _directory;
  private IndexWriter _writer;
  private BlurAnalyzer _analyzer;
  private RowIndexWriter _rowIndexWriter;
  private BlurIndexCloser _closer;
  private AtomicReference<IndexReader> _indexReaderRef = new AtomicReference<IndexReader>();
  private AtomicLong _lastCommit = new AtomicLong();
  private long _commitDelay = TimeUnit.MINUTES.toMillis(1);

  public void init() throws IOException {
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_34, _analyzer);
    config.setSimilarity(new FairSimilarity());
    config.setWriteLockTimeout(TimeUnit.MINUTES.toMillis(5));
    ((TieredMergePolicy) config.getMergePolicy()).setUseCompoundFile(false);
    _writer = new IndexWriter(_directory, config);
    _writer.commit();
//    _indexReaderRef.set(IndexReader.open(_directory));
    _indexReaderRef.set(IndexReader.open(_writer,true));
    _rowIndexWriter = new RowIndexWriter(_writer, _analyzer);
    _lastCommit.set(System.currentTimeMillis());
  }
  
  private void checkForCommit() throws IOException {
    long now = System.currentTimeMillis();
    if (_lastCommit.get() + _commitDelay < now) {
      synchronized (_writer) {
        if (_lastCommit.get() + _commitDelay < now) {
          LOG.info("Commiting [" + _directory + "]");
          _writer.commit();
          _lastCommit.set(System.currentTimeMillis());
          refresh();
        }
      }
    }
  }

  @Override
  public IndexReader getIndexReader(boolean forceRefresh) throws IOException {
    checkForCommit();
    IndexReader indexReader = _indexReaderRef.get();
    indexReader.incRef();
    return indexReader;
  }
  
  @Override
  public void close() throws IOException {
    _writer.close();
  }

  @Override
  public void deleteRow(boolean wal, String rowId) throws IOException {
    checkForCommit();
    synchronized (_writer) {
      _writer.deleteDocuments(new Term(BlurConstants.ROW_ID,rowId));
    }
  }

  @Override
  public void refresh() throws IOException {
    checkForCommit();
    synchronized (_writer) {
      IndexReader oldReader = _indexReaderRef.get();
      if (oldReader.isCurrent()) {
        return;
      }
      try {
        IndexReader reader = oldReader.reopen(_writer,true);
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
  public boolean replaceRow(boolean wal, Row row) throws IOException {
    checkForCommit();
    synchronized (_writer) {
      _rowIndexWriter.replace(wal, row);
    }
    return true;
  }

  public void setDirectory(Directory directory) {
    _directory = directory;
  }

  public void setAnalyzer(BlurAnalyzer analyzer) {
    _analyzer = analyzer;
  }

  public void setCloser(BlurIndexCloser closer) {
    _closer = closer;
  }
}
