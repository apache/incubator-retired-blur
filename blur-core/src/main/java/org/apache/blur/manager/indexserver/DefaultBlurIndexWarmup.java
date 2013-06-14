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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.warmup.IndexTracerResult;
import org.apache.blur.lucene.warmup.IndexWarmup;
import org.apache.blur.manager.indexserver.DistributedIndexServer.ReleaseReader;
import org.apache.blur.thrift.generated.ColumnPreCache;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.SegmentReader;

public class DefaultBlurIndexWarmup extends BlurIndexWarmup {

  private static final Log LOG = LogFactory.getLog(DefaultBlurIndexWarmup.class);

  @Override
  public void warmBlurIndex(final TableDescriptor table, final String shard, IndexReader reader,
      AtomicBoolean isClosed, ReleaseReader releaseReader) throws IOException {
    LOG.info("Running warmup for reader [{0}]", reader);
    try {
      int maxSampleSize = 1000;
      IndexWarmup indexWarmup = new IndexWarmup(isClosed, maxSampleSize);
      String context = table.getName() + "/" + shard;
      Map<String, List<IndexTracerResult>> sampleIndex = indexWarmup.sampleIndex(reader, context);
      ColumnPreCache columnPreCache = table.getColumnPreCache();
      if (columnPreCache != null) {
        warm(reader, columnPreCache.preCacheCols, indexWarmup, sampleIndex, context, isClosed);
      } else {
        warm(reader, getFields(reader), indexWarmup, sampleIndex, context, isClosed);
      }
    } finally {
      releaseReader.release();
    }
  }

  private Iterable<String> getFields(IndexReader reader) throws IOException {
    Set<String> fields = new TreeSet<String>();
    for (IndexReaderContext ctext : reader.getContext().leaves()) {
      AtomicReaderContext atomicReaderContext = (AtomicReaderContext) ctext;
      AtomicReader atomicReader = atomicReaderContext.reader();
      if (atomicReader instanceof SegmentReader) {
        for (String f : atomicReader.fields()) {
          fields.add(f);
        }
      }
    }
    return fields;
  }

  private void warm(IndexReader reader, Iterable<String> preCacheCols, IndexWarmup indexWarmup,
      Map<String, List<IndexTracerResult>> sampleIndex, String context, AtomicBoolean isClosed) {
    for (String field : preCacheCols) {
      try {
        indexWarmup.warm(reader, sampleIndex, field, context);
      } catch (IOException e) {
        LOG.error("Context [{0}] unknown error trying to warmup the [{1}] field", e, context, field);
      }
      if (isClosed.get()) {
        LOG.info("Context [{0}] index closed", context);
        return;
      }
    }

  }

}
