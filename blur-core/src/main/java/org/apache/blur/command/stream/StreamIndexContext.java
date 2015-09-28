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
package org.apache.blur.command.stream;

import java.io.Closeable;
import java.io.IOException;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.command.IndexContext;
import org.apache.blur.command.Shard;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.lucene.index.IndexReader;

public class StreamIndexContext extends IndexContext implements Closeable {

  private final ShardContext _shardContext;
  private final TableContext _tableContext;
  private final IndexSearcherCloseable _indexSearcher;
  private final IndexReader _indexReader;
  private final Shard _shard;

  public StreamIndexContext(BlurIndex blurIndex) throws IOException {
    _shardContext = blurIndex.getShardContext();
    _tableContext = _shardContext.getTableContext();
    _indexSearcher = blurIndex.getIndexSearcher();
    _indexReader = _indexSearcher.getIndexReader();
    _shard = new Shard(_tableContext.getTable(), _shardContext.getShard());
  }

  @Override
  public TableContext getTableContext(String table) throws IOException {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public BlurConfiguration getBlurConfiguration(String table) throws IOException {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public TableContext getTableContext() throws IOException {
    return _tableContext;
  }

  @Override
  public Shard getShard() {
    return _shard;
  }

  @Override
  public IndexSearcherCloseable getIndexSearcher() {
    return _indexSearcher;
  }

  @Override
  public IndexReader getIndexReader() {
    return _indexReader;
  }

  @Override
  public BlurConfiguration getBlurConfiguration() throws IOException {
    return _tableContext.getBlurConfiguration();
  }

  @Override
  public void close() throws IOException {
    _indexSearcher.close();
  }
}
