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

import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.utils.BlurUtil;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

public abstract class AbstractIndexServer implements IndexServer {

  public long getRecordCount(String table) throws IOException {
    long recordCount = 0;
    Map<String, BlurIndex> indexes = getIndexes(table);
    for (Map.Entry<String, BlurIndex> index : indexes.entrySet()) {
      IndexSearcherClosable searcher = null;
      try {
        searcher = index.getValue().getIndexReader();
        recordCount += searcher.getIndexReader().numDocs();
      } finally {
        if (searcher != null) {
          searcher.close();
        }
      }
    }
    return recordCount;
  }

  public long getRowCount(String table) throws IOException {
    long rowCount = 0;
    Map<String, BlurIndex> indexes = getIndexes(table);
    for (Map.Entry<String, BlurIndex> index : indexes.entrySet()) {
      IndexSearcherClosable searcher = null;
      try {
        searcher = index.getValue().getIndexReader();
        rowCount += getRowCount(searcher.getIndexReader());
      } finally {
        if (searcher != null) {
          searcher.close();
        }
      }
    }
    return rowCount;
  }

  private long getRowCount(IndexReader indexReader) throws IOException {
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TopDocs topDocs = searcher.search(new TermQuery(BlurUtil.PRIME_DOC_TERM), 1);
    return topDocs.totalHits;
  }
}
