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
package org.apache.blur.manager.writer;

import java.util.Timer;
import java.util.concurrent.ExecutorService;

import org.apache.blur.server.ShardContext;
import org.apache.blur.server.cache.ThriftCache;
import org.apache.lucene.store.Directory;

public class BlurIndexConf {

  private final ShardContext _shardContext;
  private final Directory _directory;
  private final SharedMergeScheduler _mergeScheduler;
  private final ExecutorService _searchExecutor;
  private final BlurIndexCloser _indexCloser;
  private final Timer _indexImporterTimer;
  private final Timer _bulkIndexingTimer;
  private final ThriftCache _thriftCache;
  private final Timer _indexWriterTimer;
  private final long _maxWriterIdle;

  public BlurIndexConf(ShardContext shardContext, Directory directory, SharedMergeScheduler mergeScheduler,
      ExecutorService searchExecutor, BlurIndexCloser indexCloser, Timer indexImporterTimer, Timer bulkIndexingTimer,
      ThriftCache thriftCache, Timer indexWriterTimer, long maxWriterIdle) {
    _shardContext = shardContext;
    _directory = directory;
    _mergeScheduler = mergeScheduler;
    _searchExecutor = searchExecutor;
    _indexCloser = indexCloser;
    _indexImporterTimer = indexImporterTimer;
    _bulkIndexingTimer = bulkIndexingTimer;
    _thriftCache = thriftCache;
    _indexWriterTimer = indexWriterTimer;
    _maxWriterIdle = maxWriterIdle;
  }

  public ShardContext getShardContext() {
    return _shardContext;
  }

  public Directory getDirectory() {
    return _directory;
  }

  public SharedMergeScheduler getMergeScheduler() {
    return _mergeScheduler;
  }

  public ExecutorService getSearchExecutor() {
    return _searchExecutor;
  }

  public BlurIndexCloser getIndexCloser() {
    return _indexCloser;
  }

  public Timer getIndexImporterTimer() {
    return _indexImporterTimer;
  }

  public Timer getBulkIndexingTimer() {
    return _bulkIndexingTimer;
  }

  public ThriftCache getThriftCache() {
    return _thriftCache;
  }

  public Timer getIndexWriterTimer() {
    return _indexWriterTimer;
  }

  public long getMaxWriterIdle() {
    return _maxWriterIdle;
  }

}
