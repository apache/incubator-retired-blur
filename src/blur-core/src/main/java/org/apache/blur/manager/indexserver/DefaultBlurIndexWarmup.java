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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.indexserver.DistributedIndexServer.ReleaseReader;
import org.apache.blur.thrift.generated.ColumnPreCache;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.WarmUpByFieldBounds;
import org.apache.lucene.index.WarmUpByFieldBoundsStatus;
import org.apache.lucene.util.ReaderUtil;


public class DefaultBlurIndexWarmup extends BlurIndexWarmup {

  private static final Log LOG = LogFactory.getLog(DefaultBlurIndexWarmup.class);

  @Override
  public void warmBlurIndex(final TableDescriptor table, final String shard, IndexReader reader, AtomicBoolean isClosed, ReleaseReader releaseReader) throws IOException {
    try {
      ColumnPreCache columnPreCache = table.columnPreCache;
      List<String> preCacheCols = null;
      if (columnPreCache != null) {
        preCacheCols = columnPreCache.preCacheCols;
      }
      if (preCacheCols == null) {
        LOG.info("No pre cache defined, precache all fields.");
        FieldInfos fieldInfos = ReaderUtil.getMergedFieldInfos(reader);
        preCacheCols = new ArrayList<String>();
        for (FieldInfo fieldInfo : fieldInfos) {
          if (fieldInfo.isIndexed) {
            preCacheCols.add(fieldInfo.name);
          }
        }
        preCacheCols.remove(BlurConstants.ROW_ID);
        preCacheCols.remove(BlurConstants.RECORD_ID);
        preCacheCols.remove(BlurConstants.PRIME_DOC);
        preCacheCols.remove(BlurConstants.SUPER);
      }

      WarmUpByFieldBounds warmUpByFieldBounds = new WarmUpByFieldBounds();
      WarmUpByFieldBoundsStatus status = new WarmUpByFieldBoundsStatus() {
        @Override
        public void complete(String name, Term start, Term end, long startPosition, long endPosition, long totalBytesRead, long nanoTime, AtomicBoolean isClosed) {
          double bytesPerNano = totalBytesRead / (double) nanoTime;
          double mBytesPerNano = bytesPerNano / 1024 / 1024;
          double mBytesPerSecond = mBytesPerNano * 1000000000.0;
          if (totalBytesRead > 0) {
            LOG.info("Precached field [{0}] in table [{1}] shard [{2}] file [{3}], [{4}] bytes cached at [{5} MB/s]", start.field(), table.name, shard, name, totalBytesRead,
                mBytesPerSecond);
          }
        }
      };
      if (preCacheCols != null) {
        for (String field : preCacheCols) {
          warmUpByFieldBounds.warmUpByField(isClosed, new Term(field), reader, status);
        }
      }
    } finally {
      releaseReader.release();
    }
  }

}
