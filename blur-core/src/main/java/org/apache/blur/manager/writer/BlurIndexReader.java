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
import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.warmup.TraceableDirectory;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.Row;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.BlurIndexWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

public class BlurIndexReader extends BlurIndex {

  private static final Log LOG = LogFactory.getLog(BlurIndexReader.class);

  private BlurIndexCloser _closer;
  private Directory _directory;
  private AtomicReference<DirectoryReader> _indexReaderRef = new AtomicReference<DirectoryReader>();
  private AtomicBoolean _isClosed = new AtomicBoolean(false);
  private AtomicBoolean _open = new AtomicBoolean();
  private BlurIndexRefresher _refresher;
  private final TableContext _tableContext;
  private final ShardContext _shardContext;

  public BlurIndexReader(ShardContext shardContext, Directory directory, BlurIndexRefresher refresher,
      BlurIndexCloser closer) throws IOException {
    _tableContext = shardContext.getTableContext();
    // This directory allows for warm up by adding tracing ability.
    _directory = new TraceableDirectory(directory);
    _shardContext = shardContext;
    _refresher = refresher;
    _closer = closer;

    _open.set(true);

    if (!DirectoryReader.indexExists(directory)) {
      LOG.info("Creating an empty index");
      // if the directory is empty then create an empty index.
      IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, new KeywordAnalyzer());
      conf.setWriteLockTimeout(TimeUnit.MINUTES.toMillis(5));
      new BlurIndexWriter(directory, conf).close();
    }
    _indexReaderRef.set(DirectoryReader.open(directory));
    _refresher.register(this);
  }

  @Override
  public void refresh() throws IOException {
    if (!_open.get()) {
      return;
    }
    DirectoryReader oldReader = _indexReaderRef.get();
    DirectoryReader reader = DirectoryReader.openIfChanged(oldReader);
    if (reader != null) {
      _indexReaderRef.set(reader);
      _closer.close(oldReader);
    }
  }

  @Override
  public void close() throws IOException {
    _open.set(false);
    _refresher.unregister(this);
    _directory.close();
    _isClosed.set(true);
    LOG.info("Reader for table [{0}] shard [{1}] closed.", _tableContext.getTable(), _shardContext.getShard());
  }

  @Override
  public void replaceRow(boolean waitToBeVisible, boolean wal, Row row) throws IOException {
    throw new RuntimeException("Read-only shard");
  }

  @Override
  public void deleteRow(boolean waitToBeVisible, boolean wal, String rowId) throws IOException {
    throw new RuntimeException("Read-only shard");
  }

  @Override
  public void optimize(int numberOfSegmentsPerShard) throws IOException {
    throw new RuntimeException("Read-only shard");
  }

  @Override
  public IndexSearcherClosable getIndexReader() throws IOException {
    final DirectoryReader reader = _indexReaderRef.get();
    reader.incRef();
    return new IndexSearcherClosable(reader, null) {

      @Override
      public Directory getDirectory() {
        return _directory;
      }

      @Override
      public void close() throws IOException {
        reader.decRef();
      }
    };
  }

  @Override
  public AtomicBoolean isClosed() {
    return _isClosed;
  }

  @Override
  public void createSnapshot(String name) throws IOException {
    throw new RuntimeException("Read-only shard");
  }

  @Override
  public void removeSnapshot(String name) throws IOException {
    throw new RuntimeException("Read-only shard");
  }

  @Override
  public List<String> getSnapshots() throws IOException {
    throw new RuntimeException("Read-only shard");
  }
}
