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
package org.apache.blur.manager.writer;

import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_QUEUE_MAX_PAUSE_TIME_WHEN_EMPTY;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_QUEUE_MAX_QUEUE_BATCH_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_QUEUE_MAX_WRITER_LOCK_TIME;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.lucene.index.IndexWriter;

public class MutationQueueProcessor implements Runnable, Closeable {
  
  private static final long TIMEOUT = TimeUnit.MILLISECONDS.toMillis(100);
  private final Log LOG = LogFactory.getLog(MutationQueueProcessor.class);

  private final BlockingQueue<RowMutation> _queue;
  private final BlurIndex _blurIndex;
  private final int _maxQueueBatch;
  private final long _maxProcessingTime;
  private final ShardContext _context;
  private final AtomicBoolean _running = new AtomicBoolean(false);
  private final AtomicInteger _writesWaiting;
  private final long _timeInMsThatQueueWritesPauseWhenEmpty;
  private Thread _daemonThread;

  public MutationQueueProcessor(BlockingQueue<RowMutation> queue, BlurIndex blurIndex, ShardContext context,
      AtomicInteger writesWaiting) {
    _queue = queue;
    _blurIndex = blurIndex;
    _context = context;
    TableContext tableContext = _context.getTableContext();
    BlurConfiguration blurConfiguration = tableContext.getBlurConfiguration();

    _maxQueueBatch = blurConfiguration.getInt(BLUR_SHARD_QUEUE_MAX_QUEUE_BATCH_SIZE, 100);
    _maxProcessingTime = TimeUnit.MILLISECONDS.toNanos(blurConfiguration.getInt(BLUR_SHARD_QUEUE_MAX_WRITER_LOCK_TIME,
        5000));
    _timeInMsThatQueueWritesPauseWhenEmpty = blurConfiguration
        .getLong(BLUR_SHARD_QUEUE_MAX_PAUSE_TIME_WHEN_EMPTY, 1000);
    _writesWaiting = writesWaiting;
  }

  public void startIfNotRunning() {
    synchronized (_running) {
      if (!_running.get()) {
        _running.set(true);
        _daemonThread = new Thread(this);
        _daemonThread.setDaemon(true);
        _daemonThread.setName("Queue Thread [" + _context.getTableContext().getTable() + "/" + _context.getShard()
            + "]");
        _daemonThread.start();
        LOG.info("Thread [{0}] starting.", _daemonThread.getName());
      }
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (_running) {
      if (_running.get()) {
        LOG.info("Thread [{0}] stopping.", _daemonThread.getName());
        _running.set(false);
        _daemonThread.interrupt();
        _daemonThread = null;
      }
    }
  }

  @Override
  public void run() {
    while (_running.get()) {
      try {
        MutationQueueProcessorIndexAction indexAction = new MutationQueueProcessorIndexAction();
        _blurIndex.process(indexAction);
        if (!indexAction.hadMutationsToIndex()) {
          try {
            Thread.sleep(_timeInMsThatQueueWritesPauseWhenEmpty);
          } catch (InterruptedException e) {
            return;
          }
        }
      } catch (IOException e) {
        LOG.error("Unknown error during processing of queue mutations.", e);
      }
    }
  }

  class MutationQueueProcessorIndexAction extends IndexAction {

    private final long _start = System.nanoTime();
    private boolean _didMutates = false;

    private boolean shouldContinueProcessing() {
      if (_start + _maxProcessingTime < System.nanoTime()) {
        return false;
      }
      if (_writesWaiting.get() > 0) {
        return false;
      }
      return true;
    }

    public boolean hadMutationsToIndex() {
      return _didMutates;
    }

    @Override
    public void performMutate(IndexSearcherCloseable searcher, IndexWriter writer) throws IOException {
      List<RowMutation> lst = new ArrayList<RowMutation>();
      while (shouldContinueProcessing()) {
        if (_queue.drainTo(lst, _maxQueueBatch) > 0) {
          try {
            List<RowMutation> reduceMutates = MutatableAction.reduceMutates(lst);
            MutatableAction mutatableAction = new MutatableAction(_context);
            mutatableAction.mutate(reduceMutates);
            LOG.debug("Mutating [{0}]", reduceMutates.size());
            mutatableAction.performMutate(searcher, writer);
            _didMutates = true;
          } catch (BlurException e) {
            LOG.error("Unknown error during reduce of mutations.", e);
          }
        } else {
          _didMutates = false;
        }
        lst.clear();
        if (!_didMutates) {
          synchronized (_queue) {
            try {
              _queue.wait(TIMEOUT);
            } catch (InterruptedException e) {
              throw new IOException(e);
            }
          }
        }
      }
    }

    @Override
    public void doPreCommit(IndexSearcherCloseable indexSearcher, IndexWriter writer) throws IOException {

    }

    @Override
    public void doPostCommit(IndexWriter writer) throws IOException {

    }

    @Override
    public void doPreRollback(IndexWriter writer) throws IOException {

    }

    @Override
    public void doPostRollback(IndexWriter writer) throws IOException {

    }

  }

}
