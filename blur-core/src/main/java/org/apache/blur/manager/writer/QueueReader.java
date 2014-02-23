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

import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_QUEUE_READER_BACKOFF;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_QUEUE_READER_MAX;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.RowMutation;

public abstract class QueueReader implements Closeable, Runnable {

  private static final Log LOG = LogFactory.getLog(QueueReader.class);

  protected final ShardContext _shardContext;
  protected final BlurIndex _index;
  protected final long _backOff;
  protected final Thread _daemon;
  protected final AtomicBoolean _running = new AtomicBoolean();
  protected final int _max;
  protected final TableContext _tableContext;

  public QueueReader(BlurIndex index, ShardContext shardContext) {
    _running.set(true);
    _index = index;
    _shardContext = shardContext;
    _tableContext = _shardContext.getTableContext();
    BlurConfiguration configuration = _tableContext.getBlurConfiguration();
    _backOff = configuration.getLong(BLUR_SHARD_INDEX_QUEUE_READER_BACKOFF, 500);
    _max = configuration.getInt(BLUR_SHARD_INDEX_QUEUE_READER_MAX, 500);
    _daemon = new Thread(this);
    _daemon.setName("Queue Loader for [" + _tableContext.getTable() + "/" + shardContext.getShard() + "]");
    _daemon.setDaemon(true);
    _daemon.start();
  }

  @Override
  public void run() {
    List<RowMutation> mutations = new ArrayList<RowMutation>();
    while (_running.get()) {
      take(mutations, _max);
      if (mutations.isEmpty()) {
        try {
          Thread.sleep(_backOff);
        } catch (InterruptedException e) {
          return;
        }
      } else {
        MutatableAction mutatableAction = new MutatableAction(_shardContext);
        mutatableAction.mutate(mutations);
        try {
          _index.process(mutatableAction);
          success();
        } catch (IOException e) {
          failure();
          LOG.error("Unknown error during loading of rowmutations from queue [{0}] into table [{1}] and shard [{2}].",
              this.toString(), _tableContext.getTable(), _shardContext.getShard());
        } finally {
          mutations.clear();
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (_running.get()) {
      _running.set(false);
      _daemon.interrupt();
    }
  }

  /**
   * Takes up to the max number of {@link RowMutation}s off the queue and
   * returns. The implementation can choose to block until new items are
   * available. However if the method returns without adding any items to the
   * mutations list, the loading thread will back off a configurable amount of
   * time. <br/>
   * <br/>
   * Configuration setting: &quot;blur.shard.index.queue.reader.backoff&quot;
   * 
   * @param mutations
   * @param max
   */
  public abstract void take(List<RowMutation> mutations, int max);

  /**
   * This method will be called after each successful load of data from the
   * queue. This will allow the queue to be notified that the information has
   * been successfully loaded.
   */
  public abstract void success();

  /**
   * This method will be called after each failed load of data from the queue.
   * This will allow the queue to be notified that the information has been WAS
   * NOT successfully loaded.
   */
  public abstract void failure();

}
