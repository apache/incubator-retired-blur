package org.apache.blur.server;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;

public abstract class IndexSearcherCloseableBase extends IndexSearcher implements IndexSearcherCloseable {

  public IndexSearcherCloseableBase(IndexReader r, ExecutorService executor) {
    super(r, executor);
  }

  public abstract Directory getDirectory();

  @Override
  public abstract void close() throws IOException;

  protected void search(List<AtomicReaderContext> leaves, Weight weight, Collector collector) throws IOException {
    // TODO: should we make this
    // threaded...? the Collector could be sync'd?
    // always use single thread:
    for (AtomicReaderContext ctx : leaves) { // search each subreader
      Tracer trace = Trace.trace("search - internal", Trace.param("AtomicReader", ctx.reader()));
      try {
        try {
          collector.setNextReader(ctx);
        } catch (CollectionTerminatedException e) {
          // there is no doc of interest in this reader context
          // continue with the following leaf
          continue;
        }
        Scorer scorer = weight.scorer(ctx, !collector.acceptsDocsOutOfOrder(), true, ctx.reader().getLiveDocs());
        if (scorer != null) {
          try {
            scorer.score(collector);
          } catch (CollectionTerminatedException e) {
            // collection was terminated prematurely
            // continue with the following leaf
          }
        }
      } finally {
        trace.done();
      }
    }
  }

}
