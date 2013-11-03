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
import java.io.IOException;
import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.store.AlreadyClosedException;

public class BlurIndexRefresher extends TimerTask implements Closeable {

  private static final Log LOG = LogFactory.getLog(BlurIndexRefresher.class);

  private Timer _timer;
  private long _period = TimeUnit.MINUTES.toMillis(1);
  private long _delay = _period;
  private Collection<BlurIndex> _indexes = new LinkedBlockingQueue<BlurIndex>();
  
  public BlurIndexRefresher() {
    _timer = new Timer("IndexReader-Refresher", true);
    _timer.schedule(this, _delay, _period);
    LOG.info("Init Complete");
  }

  public void register(BlurIndex blurIndex) {
    _indexes.add(blurIndex);
  }

  public void unregister(BlurIndex blurIndex) {
    _indexes.remove(blurIndex);
  }

  public void close() {
    _timer.purge();
    _timer.cancel();
  }

  @Override
  public void run() {
    try {
      refreshInternal();
    } catch (Throwable e) {
      LOG.error("Unknown error", e);
    }
  }

  private void refreshInternal() {
    for (BlurIndex index : _indexes) {
      try {
        index.refresh();
      } catch (IOException e) {
        LOG.error("Unknown error while refreshing index of writer [{0}]", e, index);
      } catch (AlreadyClosedException e) {
        LOG.warn("Index has already been closed [{0}]", e, index);
      }
    }
  }

  public void setPeriod(long period) {
    _period = period;
  }

  public void setDelay(long delay) {
    _delay = delay;
  }

}
