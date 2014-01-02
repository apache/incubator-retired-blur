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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.indexserver.BlurIndexWarmup;
import org.apache.blur.manager.indexserver.DistributedIndexServer.ReleaseReader;
import org.apache.blur.server.ShardContext;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;

public class BlurIndexReaderWarmer extends IndexReaderWarmer {

  private static final Log LOG = LogFactory.getLog(BlurIndexReaderWarmer.class);

  private final AtomicBoolean _isClosed;
  private final AtomicLong _pause = new AtomicLong();
  private final BlurIndexWarmup _indexWarmup;
  private final TableDescriptor _table;
  private final String _shard;

  public BlurIndexReaderWarmer(ShardContext shardContext, AtomicBoolean isClosed, BlurIndexWarmup indexWarmup) {
    _isClosed = isClosed;
    _table = shardContext.getTableContext().getDescriptor();
    _shard = shardContext.getShard();
    _indexWarmup = indexWarmup;
  }

  @Override
  public void warm(AtomicReader reader) throws IOException {
    LOG.debug("Warming reader [{0}]", reader);
    ReleaseReader releaseReader = new ReleaseReader() {
      @Override
      public void release() throws IOException {

      }
    };
    _indexWarmup.warmBlurIndex(_table, _shard, reader, _isClosed, releaseReader, _pause);
  }
}
