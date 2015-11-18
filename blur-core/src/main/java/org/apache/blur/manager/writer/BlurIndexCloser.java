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
import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.index.IndexReader;

public class BlurIndexCloser implements Runnable, Closeable {

  private static final Log LOG = LogFactory.getLog(BlurIndexCloser.class);
  private static final long PAUSE_TIME = TimeUnit.SECONDS.toMillis(1);
  private final Thread daemon;
  private final Collection<IndexReader> readers = new LinkedBlockingQueue<IndexReader>();
  private final AtomicBoolean running = new AtomicBoolean();
  private final ExecutorService executorService;

  public BlurIndexCloser() {
    running.set(true);
    daemon = new Thread(this);
    daemon.setDaemon(true);
    daemon.setName(getClass().getName() + "-Daemon");
    daemon.start();
    LOG.info("Init Complete");
    executorService = Executors.newThreadPool("Blur Index Closer Pool", 10);
  }

  public int getReadersToBeClosedCount() {
    return readers.size();
  }

  public void close() {
    running.set(false);
    daemon.interrupt();
    executorService.shutdownNow();
  }

  public void close(IndexReader reader) {
    readers.add(reader);
  }

  @Override
  public void run() {
    while (running.get()) {
      try {
        tryToCloseReaders();
      } catch (Throwable t) {
        LOG.error("Unknown error", t);
      }
      try {
        Thread.sleep(PAUSE_TIME);
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  private void tryToCloseReaders() {
    LOG.debug("Trying to close [{0}] readers", readers.size());
    Iterator<IndexReader> it = readers.iterator();
    while (it.hasNext()) {
      IndexReader reader = it.next();
      if (reader.getRefCount() == 1) {
        it.remove();
        closeInternal(reader);
      } else {
        LOG.debug("Could not close indexreader [" + reader + "] because of ref count [" + reader.getRefCount() + "].");
      }
    }
  }

  private void closeInternal(final IndexReader reader) {
    if (reader.getRefCount() == 0) {
      // Already closed.
      return;
    }
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          long s = System.currentTimeMillis();
          reader.close();
          long e = System.currentTimeMillis();
          LOG.debug("Size [{0}] time to close [{1}] Closing indexreader [{2}].", readers.size(), (e - s), reader);
        } catch (Exception e) {
          readers.add(reader);
          LOG.error("Error while trying to close indexreader [" + reader + "].", e);
        }
      }
    });
  }
}