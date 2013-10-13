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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

public class IndexSearcherClosableNRT extends IndexSearcherClosable {

  private final AtomicReference<SearcherManager> _nrtManagerRef;
  private final Directory _directory;

  public IndexSearcherClosableNRT(IndexReader r, ExecutorService executor, AtomicReference<SearcherManager> nrtManagerRef,
      Directory directory) {
    super(r, executor);
    _nrtManagerRef = nrtManagerRef;
    _directory = directory;
  }
  
  @Override
  public Directory getDirectory() {
    return _directory;
  }

  @Override
  public void close() throws IOException {
    _nrtManagerRef.get().release(this);
  }

}
