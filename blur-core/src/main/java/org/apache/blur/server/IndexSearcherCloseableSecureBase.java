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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import lucene.security.index.AccessControlFactory;
import lucene.security.search.SecureIndexSearcher;

import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;

public abstract class IndexSearcherCloseableSecureBase extends SecureIndexSearcher implements IndexSearcherCloseable {

  public IndexSearcherCloseableSecureBase(IndexReader r, ExecutorService executor,
      AccessControlFactory accessControlFactory, Collection<String> readAuthorizations,
      Collection<String> discoverAuthorizations, Set<String> discoverableFields) throws IOException {
    super(r, executor, accessControlFactory, readAuthorizations, discoverAuthorizations, discoverableFields);
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
        super.search(makeList(ctx), weight, collector);
      } finally {
        trace.done();
      }
    }
  }

  private List<AtomicReaderContext> makeList(AtomicReaderContext ctx) {
    return Arrays.asList(ctx);
  }

}
