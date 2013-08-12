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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.warmup.IndexTracerResult;
import org.apache.blur.lucene.warmup.IndexWarmup;
import org.apache.blur.server.ShardContext;
import org.apache.blur.thrift.generated.ColumnPreCache;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;

public class FieldBasedWarmer extends IndexReaderWarmer {

  private static final Log LOG = LogFactory.getLog(FieldBasedWarmer.class);

  private ShardContext shardContext;
  private AtomicBoolean isClosed;

  public FieldBasedWarmer(ShardContext shardContext, AtomicBoolean isClosed) {
    this.isClosed = isClosed;
    this.shardContext = shardContext;
  }

  @Override
  public void warm(AtomicReader reader) throws IOException {
    ColumnPreCache columnPreCache = shardContext.getTableContext().getDescriptor().getColumnPreCache();
    List<String> preCacheCols = columnPreCache == null ? null : columnPreCache.getPreCacheCols();
    int maxSampleSize = 1000;
    IndexWarmup indexWarmup = new IndexWarmup(isClosed, maxSampleSize);
    String context = shardContext.getTableContext().getTable() + "/" + shardContext.getShard();
    Map<String, List<IndexTracerResult>> sampleIndex = indexWarmup.sampleIndex(reader, context);
    if (preCacheCols != null) {
      warm(reader, columnPreCache.preCacheCols, indexWarmup, sampleIndex, context, isClosed);
    } else {
      Fields fields = reader.fields();
      warm(reader, fields, indexWarmup, sampleIndex, context, isClosed);
    }
  }

  private void warm(AtomicReader reader, Iterable<String> preCacheCols, IndexWarmup indexWarmup,
      Map<String, List<IndexTracerResult>> sampleIndex, String context, AtomicBoolean isClosed) {
    for (String field : preCacheCols) {
      try {
        indexWarmup.warm(reader, sampleIndex, field, context);
      } catch (IOException e) {
        LOG.error("Context [{0}] unknown error trying to warmup the [{1}] field", e, context, field);
        LOG.error("Current sampleIndex [{0}]",sampleIndex);
      }
      if (isClosed.get()) {
        LOG.info("Context [{0}] index closed", context);
        return;
      }
    }
  }
}
