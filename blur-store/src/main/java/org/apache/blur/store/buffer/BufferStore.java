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
import static org.apache.blur.metrics.MetricsConstants.OTHER_SIZES_ALLOCATED;
import static org.apache.blur.metrics.MetricsConstants._1K_SIZE_ALLOCATED;
import static org.apache.blur.metrics.MetricsConstants._8K_SIZE_ALLOCATED;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class BufferStore {

  private static final Log LOG = LogFactory.getLog(BufferStore.class);

  private static BlockingQueue<byte[]> _1024;
  private static BlockingQueue<byte[]> _8192;

  private static Meter _lost;
  private static Meter _8K;
  private static Meter _1K;
  private static Meter _other;
  private volatile static boolean setup = false;

  public static void init(int _1KSize, int _8KSize) {
    if (!setup) {
      _lost = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, LUCENE, LOST, INTERNAL_BUFFERS), INTERNAL_BUFFERS, TimeUnit.SECONDS);
      _8K = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, LUCENE, _1K_SIZE_ALLOCATED, INTERNAL_BUFFERS), INTERNAL_BUFFERS, TimeUnit.SECONDS);
      _1K = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, LUCENE, _8K_SIZE_ALLOCATED, INTERNAL_BUFFERS), INTERNAL_BUFFERS, TimeUnit.SECONDS);
      _other = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, LUCENE, OTHER_SIZES_ALLOCATED, INTERNAL_BUFFERS), INTERNAL_BUFFERS, TimeUnit.SECONDS);
      LOG.info("Initializing the 1024 buffers with [{0}] buffers.", _1KSize);
      _1024 = setupBuffers(1024, _1KSize, _1K);
      LOG.info("Initializing the 8192 buffers with [{0}] buffers.", _8KSize);
      _8192 = setupBuffers(8192, _8KSize, _8K);
      setup = true;
    }
  }

  private static BlockingQueue<byte[]> setupBuffers(int bufferSize, int count, Meter meter) {
    BlockingQueue<byte[]> queue = new ArrayBlockingQueue<byte[]>(count);
    for (int i = 0; i < count; i++) {
      meter.mark();
      queue.add(new byte[bufferSize]);
    }
    return queue;
  }

  public static byte[] takeBuffer(int bufferSize) {
    switch (bufferSize) {
    case 1024:
      return newBuffer1024(_1024.poll());
    case 8192:
      return newBuffer8192(_8192.poll());
    default:
      return newBuffer(bufferSize);
    }
  }

  public static void putBuffer(byte[] buffer) {
    if (buffer == null) {
      return;
    }
    int bufferSize = buffer.length;
    switch (bufferSize) {
    case 1024:
      checkReturn(_1024.offer(buffer));
      return;
    case 8192:
      checkReturn(_8192.offer(buffer));
      return;
    }
  }

  private static void checkReturn(boolean offer) {
    if (!offer) {
      _lost.mark();
    }
  }

  private static byte[] newBuffer1024(byte[] buf) {
    if (buf != null) {
      return buf;
    }
    _1K.mark();
    return new byte[1024];
  }

  private static byte[] newBuffer8192(byte[] buf) {
    if (buf != null) {
      return buf;
    }
    _8K.mark();
    return new byte[8192];
  }

  private static byte[] newBuffer(int size) {
    _other.mark();
    return new byte[size];
  }
}
