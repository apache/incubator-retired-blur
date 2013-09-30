package org.apache.blur.lucene.store.refcounter;

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
import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

public class IndexInputCloser implements Runnable, Closeable {

  private static final Log LOG = LogFactory.getLog(IndexInputCloser.class);

  private ReferenceQueue<IndexInput> referenceQueue = new ReferenceQueue<IndexInput>();
  private Thread daemon;
  private AtomicBoolean running = new AtomicBoolean();
  private Collection<IndexInputCloserRef> refs = Collections
      .newSetFromMap(new ConcurrentHashMap<IndexInputCloserRef, Boolean>());

  static class IndexInputCloserRef extends WeakReference<IndexInput> implements Closeable {
    public IndexInputCloserRef(IndexInput referent, ReferenceQueue<? super IndexInput> q) {
      super(referent, q);
    }

    @Override
    public void close() throws IOException {
      IndexInput input = get();
      if (input != null) {
        LOG.debug("Closing indexinput [{0}]", input);
        input.close();
      }
    }
  }

  public void init() {
    running.set(true);
    daemon = new Thread(this);
    daemon.setDaemon(true);
    daemon.setName("IndexIndexCloser");
    daemon.start();
  }

  public void add(IndexInput indexInput) {
    LOG.debug("Adding [{0}]", indexInput);
    IndexInputCloserRef ref = new IndexInputCloserRef(indexInput, referenceQueue);
    refs.add(ref);
  }

  public void close() {
    running.set(false);
    refs.clear();
    daemon.interrupt();
  }

  @Override
  public void run() {
    while (running.get()) {
      try {
        IndexInputCloserRef ref = (IndexInputCloserRef) referenceQueue.remove();
        LOG.debug("Closing [{0}]", ref);
        IOUtils.closeWhileHandlingException(ref);
        refs.remove(ref);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted");
        running.set(false);
        return;
      }
    }
  }

}
