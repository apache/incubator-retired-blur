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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.lucene.store.refcounter.DirectoryReferenceFileGC;
import org.apache.blur.manager.indexserver.BlurIndexWarmup;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.utils.BlurUtil;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

public abstract class BlurIndex {

  private static final long ONE_MINUTE = TimeUnit.MINUTES.toMillis(1);
  private long _lastMemoryCheck = 0;
  private long _memoryUsage = 0;
  protected ShardContext _shardContext;

  public BlurIndex(ShardContext shardContext, Directory directory, SharedMergeScheduler mergeScheduler,
      DirectoryReferenceFileGC gc, ExecutorService searchExecutor, BlurIndexCloser indexCloser,
      BlurIndexRefresher refresher, BlurIndexWarmup indexWarmup) throws IOException {
    _shardContext = shardContext;
  }

  public abstract void replaceRow(boolean waitToBeVisible, boolean wal, Row row) throws IOException;

  public abstract void deleteRow(boolean waitToBeVisible, boolean wal, String rowId) throws IOException;

  public abstract IndexSearcherClosable getIndexSearcher() throws IOException;

  public abstract void close() throws IOException;

  public abstract void refresh() throws IOException;

  public abstract AtomicBoolean isClosed();

  public abstract void optimize(int numberOfSegmentsPerShard) throws IOException;

  public abstract void createSnapshot(String name) throws IOException;

  public abstract void removeSnapshot(String name) throws IOException;

  public abstract List<String> getSnapshots() throws IOException;

  public long getRecordCount() throws IOException {
    IndexSearcherClosable searcher = getIndexSearcher();
    try {
      return searcher.getIndexReader().numDocs();
    } finally {
      if (searcher != null) {
        searcher.close();
      }
    }
  }

  public long getRowCount() throws IOException {
    IndexSearcherClosable searcher = getIndexSearcher();
    try {
      return getRowCount(searcher);
    } finally {
      if (searcher != null) {
        searcher.close();
      }
    }
  }

  protected long getRowCount(IndexSearcher searcher) throws IOException {
    TopDocs topDocs = searcher.search(new TermQuery(BlurUtil.PRIME_DOC_TERM), 1);
    return topDocs.totalHits;
  }

  public long getIndexMemoryUsage() throws IOException {
    return 0;
    // long now = System.currentTimeMillis();
    // if (_lastMemoryCheck + ONE_MINUTE > now) {
    // return _memoryUsage;
    // }
    // IndexSearcherClosable searcher = getIndexReader();
    // try {
    // IndexReaderContext topReaderContext = searcher.getTopReaderContext();
    // return _memoryUsage = RamUsageEstimator.sizeOf(topReaderContext, new
    // ClassNameFilter() {
    // @Override
    // public boolean include(String className) {
    // if (className.startsWith("org.apache.blur.index.ExitableReader")) {
    // return true;
    // } else if (className.startsWith("org.apache.blur.")) {
    // // System.out.println("className [" + className + "]");
    // return false;
    // }
    // return true;
    // }
    // });
    // } finally {
    // searcher.close();
    // _lastMemoryCheck = System.currentTimeMillis();
    // }
  }

  public long getSegmentCount() throws IOException {
    IndexSearcherClosable indexSearcherClosable = getIndexSearcher();
    try {
      IndexReader indexReader = indexSearcherClosable.getIndexReader();
      IndexReaderContext context = indexReader.getContext();
      return context.leaves().size();
    } finally {
      indexSearcherClosable.close();
    }
  }

  public ShardContext getShardContext() {
    return _shardContext;
  }

  public abstract void process(MutatableAction mutatableAction) throws IOException;

}
