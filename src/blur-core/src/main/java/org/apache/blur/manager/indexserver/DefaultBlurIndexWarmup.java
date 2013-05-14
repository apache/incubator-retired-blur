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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.indexserver.DistributedIndexServer.ReleaseReader;
import org.apache.blur.manager.writer.FieldBasedWarmer;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;

public class DefaultBlurIndexWarmup extends BlurIndexWarmup {

  private static final Log LOG = LogFactory.getLog(DefaultBlurIndexWarmup.class);

  @Override
  public void warmBlurIndex(final TableDescriptor table, final String shard, IndexReader reader,
      AtomicBoolean isClosed, ReleaseReader releaseReader) throws IOException {
    LOG.info("Running warmup for reader [{0}]", reader);
    try {
      FieldBasedWarmer warmer = new FieldBasedWarmer(table);
      for (IndexReaderContext context : reader.getContext().leaves()) {
        AtomicReaderContext atomicReaderContext = (AtomicReaderContext) context;
        AtomicReader atomicReader = atomicReaderContext.reader();
        warmer.warm(atomicReader);
      }
    } finally {
      releaseReader.release();
    }
  }

}
