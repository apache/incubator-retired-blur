package org.apache.blur.manager.indexserver;

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
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;


public abstract class AbstractIndexServer implements IndexServer {

  private Map<String, IndexCounts> _recordsTableCounts = new ConcurrentHashMap<String, IndexCounts>();
  private Map<String, IndexCounts> _rowTableCounts = new ConcurrentHashMap<String, IndexCounts>();

  private static class IndexCounts {
    Map<String, IndexCount> counts = new ConcurrentHashMap<String, IndexCount>();
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
          if (!isValid(indexCount, indexReader)) {
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
          if (!isValid(indexCount, indexReader)) {
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
