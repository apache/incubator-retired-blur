package org.apache.blur.store.buffer;

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
import static org.apache.blur.metrics.MetricsConstants.INTERNAL_BUFFERS;
import static org.apache.blur.metrics.MetricsConstants.LOST;
import static org.apache.blur.metrics.MetricsConstants.LUCENE;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.SIZE_ALLOCATED;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class BufferStore implements Store {

  private static final Log LOG = LogFactory.getLog(BufferStore.class);

  private static final Store EMPTY = new Store() {

    private final Meter _emptyCreated;
    private final Meter _emptyLost;

    {
      _emptyCreated = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, LUCENE, "Not Configured " + SIZE_ALLOCATED,
          INTERNAL_BUFFERS), INTERNAL_BUFFERS, TimeUnit.SECONDS);
      _emptyLost = Metrics.newMeter(
          new MetricName(ORG_APACHE_BLUR, LUCENE, "Not Configured " + LOST, INTERNAL_BUFFERS), INTERNAL_BUFFERS,
          TimeUnit.SECONDS);
    }

    @Override
    public byte[] takeBuffer(int bufferSize) {
      _emptyCreated.mark();
      return new byte[bufferSize];
    }

    @Override
    public void putBuffer(byte[] buffer) {
      _emptyLost.mark();
    }
  };

  private final static ConcurrentMap<Integer, BufferStore> _bufferStores = new ConcurrentHashMap<Integer, BufferStore>();

  private final BlockingQueue<byte[]> _buffers;
  private final Meter _created;
  private final Meter _lost;
  private final int _bufferSize;

  public synchronized static void initNewBuffer(int bufferSize, long totalAmount) {
    BufferStore bufferStore = _bufferStores.get(bufferSize);
    if (bufferStore == null) {
      long count = totalAmount / bufferSize;
      if (count > Integer.MAX_VALUE) {
        count = Integer.MAX_VALUE;
      }
      BufferStore store = new BufferStore(bufferSize, (int) count);
      _bufferStores.put(bufferSize, store);
    } else {
      LOG.warn("Buffer store for size [{0}] already setup.", bufferSize);
    }
  }

  private BufferStore(int bufferSize, int count) {
    _bufferSize = bufferSize;
    _created = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, LUCENE, bufferSize + " " + SIZE_ALLOCATED,
        INTERNAL_BUFFERS), INTERNAL_BUFFERS, TimeUnit.SECONDS);
    _lost = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, LUCENE, bufferSize + " " + LOST, INTERNAL_BUFFERS),
        INTERNAL_BUFFERS, TimeUnit.SECONDS);
    _buffers = setupBuffers(bufferSize, count, _created);
  }

  private static BlockingQueue<byte[]> setupBuffers(int bufferSize, int count, Meter meter) {
    BlockingQueue<byte[]> queue = new ArrayBlockingQueue<byte[]>(count);
    for (int i = 0; i < count; i++) {
      meter.mark();
      queue.add(new byte[bufferSize]);
    }
    return queue;
  }

  public static Store instance(int bufferSize) {
    BufferStore bufferStore = _bufferStores.get(bufferSize);
    if (bufferStore == null) {
      return EMPTY;
    }
    return bufferStore;
  }

  @Override
  public byte[] takeBuffer(int bufferSize) {
    if (bufferSize != _bufferSize) {
      throw new RuntimeException("Buffer with length [" + bufferSize + "] does not match buffer size of ["
          + _bufferSize + "]");
    }
    return newBuffer(_buffers.poll());
  }

  @Override
  public void putBuffer(byte[] buffer) {
    if (buffer == null) {
      return;
    }
    if (buffer.length != _bufferSize) {
      throw new RuntimeException("Buffer with length [" + buffer.length + "] does not match buffer size of ["
          + _bufferSize + "]");
    }
    checkReturn(_buffers.offer(buffer));
  }

  private void checkReturn(boolean offer) {
    if (!offer) {
      _lost.mark();
    }
  }

  private byte[] newBuffer(byte[] buf) {
    if (buf != null) {
      return buf;
    }
    _created.mark();
    return new byte[_bufferSize];
  }
}
