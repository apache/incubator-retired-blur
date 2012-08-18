package com.nearinfinity.blur.manager.indexserver;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;

import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.writer.BlurIndex;
import com.nearinfinity.blur.utils.BlurConstants;

public abstract class AbstractIndexServer implements IndexServer {
  
  private Map<String,IndexCounts> _recordsTableCounts = new ConcurrentHashMap<String, IndexCounts>();
  private Map<String,IndexCounts> _rowTableCounts = new ConcurrentHashMap<String, IndexCounts>();
  
  private static class IndexCounts {
    Map<String,IndexCount> counts = new ConcurrentHashMap<String, IndexCount>();
  }
  
  private static class IndexCount {
    long version;
    long count = -1;
  }

  public long getRecordCount(String table) throws IOException {
    IndexCounts indexCounts;
    synchronized (_recordsTableCounts) {
      indexCounts = _recordsTableCounts.get(table);
      if (indexCounts == null) {
        indexCounts = new IndexCounts();
        _recordsTableCounts.put(table, indexCounts);
      }
    }
    synchronized (indexCounts) {
      long recordCount = 0;
      Map<String, BlurIndex> indexes = getIndexes(table);
      for (Map.Entry<String, BlurIndex> index : indexes.entrySet()) {
        IndexReader indexReader = null;
        try {
          String shard = index.getKey();
          IndexCount indexCount = indexCounts.counts.get(shard);
          if (indexCount == null) {
            indexCount = new IndexCount();
            indexCounts.counts.put(shard, indexCount);
          }
          indexReader = index.getValue().getIndexReader();
          if (!isValid(indexCount,indexReader)) {
            indexCount.count = indexReader.numDocs();
            indexCount.version = indexReader.getVersion();
          }
          recordCount += indexCount.count;
        } finally {
          if (indexReader != null) {
            indexReader.decRef();
          }
        }
      }
      return recordCount;
    }
  }

  private boolean isValid(IndexCount indexCount, IndexReader indexReader) {
    if (indexCount.version == indexReader.getVersion() && indexCount.count != -1l) {
      return true;
    }
    return false;
  }

  public long getRowCount(String table) throws IOException {
    IndexCounts indexCounts;
    synchronized (_rowTableCounts) {
      indexCounts = _rowTableCounts.get(table);
      if (indexCounts == null) {
        indexCounts = new IndexCounts();
        _rowTableCounts.put(table, indexCounts);
      }
    }
    synchronized (indexCounts) {
      long rowCount = 0;
      Map<String, BlurIndex> indexes = getIndexes(table);
      for (Map.Entry<String, BlurIndex> index : indexes.entrySet()) {
        IndexReader indexReader = null;
        try {
          String shard = index.getKey();
          IndexCount indexCount = indexCounts.counts.get(shard);
          if (indexCount == null) {
            indexCount = new IndexCount();
            indexCounts.counts.put(shard, indexCount);
          }
          indexReader = index.getValue().getIndexReader();
          if (!isValid(indexCount,indexReader)) {
            indexCount.count = getRowCount(indexReader);
            indexCount.version = indexReader.getVersion();
          }
          rowCount += indexCount.count;
        } finally {
          if (indexReader != null) {
            indexReader.decRef();
          }
        }
      }
      return rowCount;
    }
  }

  private long getRowCount(IndexReader indexReader) throws IOException {
    long rowCount = 0;
    TermDocs termDocs = indexReader.termDocs(BlurConstants.PRIME_DOC_TERM);
    while (termDocs.next()) {
      if (!indexReader.isDeleted(termDocs.doc())) {
        rowCount++;
      }
    }
    termDocs.close();
    return rowCount;
  }
}
