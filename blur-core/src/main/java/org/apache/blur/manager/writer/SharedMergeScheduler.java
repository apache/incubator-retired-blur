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
import static org.apache.blur.utils.BlurConstants.SHARED_MERGE_SCHEDULER;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;

public class SharedMergeScheduler implements Runnable, Closeable {

  private static final Log LOG = LogFactory.getLog(SharedMergeScheduler.class);
  private static final long ONE_SECOND = 1000;

  private BlockingQueue<IndexWriter> _writers = new LinkedBlockingQueue<IndexWriter>();
  private AtomicBoolean _running = new AtomicBoolean(true);
  private ExecutorService _service;

  public SharedMergeScheduler(int threads) {
    _service = Executors.newThreadPool(SHARED_MERGE_SCHEDULER, threads, false);
    for (int i = 0; i < threads; i++) {
      _service.submit(this);
    }
  }

  private void mergeIndexWriter(IndexWriter writer) {
    synchronized (_writers) {
      if (!_writers.contains(writer)) {
        LOG.debug("Adding writer to merge [{0}]", writer);
        _writers.add(writer);
      }
    }
  }

  private void removeWriter(IndexWriter writer) {
    synchronized (_writers) {
      _writers.remove(writer);
    }
  }

  public MergeScheduler getMergeScheduler() {
    return new MergeScheduler() {

      private IndexWriter _writer;

      @Override
      public void merge(IndexWriter writer) throws IOException {
        _writer = writer;
        mergeIndexWriter(writer);
      }

      @Override
      public void close() throws IOException {
        removeWriter(_writer);
      }
    };
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    _service.shutdownNow();
  }

  @Override
  public void run() {
    while (_running.get()) {
      try {
        IndexWriter writer;
        synchronized (_writers) {
          writer = _writers.poll();
        }
        if (writer == null) {
          synchronized (this) {
            wait(ONE_SECOND);
          }
        } else if (performMergeWriter(writer)) {
          // there seems to be more merges to do
          mergeIndexWriter(writer);
        }
      } catch (InterruptedException e) {
        LOG.debug("Merging interrupted, exiting.");
        return;
      } catch (IOException e) {
        LOG.error("Unknown IOException", e);
      }
    }
  }

  private boolean performMergeWriter(IndexWriter writer) throws IOException {
    MergePolicy.OneMerge merge = writer.getNextMerge();
    if (merge == null) {
      LOG.debug("No merges to run for [{0}]", writer);
      return false;
    }
    long s = System.currentTimeMillis();
    writer.merge(merge);
    long e = System.currentTimeMillis();
    double time = (e - s) / 1000.0;
    double rate = (merge.totalBytesSize() / 1024 / 1024) / time;
    if (time > 10) {
      LOG.info("Merge took [{0} s] to complete at rate of [{1} MB/s]", time, rate);
    } else {
      LOG.debug("Merge took [{0} s] to complete at rate of [{1} MB/s]", time, rate);
    }
    return true;
  }

}
