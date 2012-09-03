package com.nearinfinity.blur.store;

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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.metrics.BlurMetrics;

public class BufferStore {

  private static final Log LOG = LogFactory.getLog(BufferStore.class);

  private static BlockingQueue<byte[]> _1024 = setupBuffers(1024, 1);
  private static BlockingQueue<byte[]> _8192 = setupBuffers(8192, 1);
  public static AtomicLong _blurShardBuffercacheLost = new AtomicLong();
  public static AtomicLong _blurShardBuffercacheAllocate1024 = new AtomicLong();
  public static AtomicLong _blurShardBuffercacheAllocate8192 = new AtomicLong();
  public static AtomicLong _blurShardBuffercacheAllocateOther = new AtomicLong();

  public static void init(BlurConfiguration configuration, BlurMetrics metrics) {
    int _1024Size = configuration.getInt("blur.shard.buffercache.1024", 8192);
    int _8192Size = configuration.getInt("blur.shard.buffercache.8192", 8192);
    LOG.info("Initializing the 1024 buffers with [{0}] buffers.", _1024Size);
    _1024 = setupBuffers(1024, _1024Size);
    LOG.info("Initializing the 8192 buffers with [{0}] buffers.", _8192Size);
    _8192 = setupBuffers(8192, _8192Size);
    _blurShardBuffercacheLost = metrics.blurShardBuffercacheLost;
    _blurShardBuffercacheAllocate1024 = metrics.blurShardBuffercacheAllocate1024;
    _blurShardBuffercacheAllocate8192 = metrics.blurShardBuffercacheAllocate8192;
    _blurShardBuffercacheAllocateOther = metrics.blurShardBuffercacheAllocateOther;
  }

  private static BlockingQueue<byte[]> setupBuffers(int bufferSize, int count) {
    BlockingQueue<byte[]> queue = new ArrayBlockingQueue<byte[]>(count);
    for (int i = 0; i < count; i++) {
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
      _blurShardBuffercacheLost.incrementAndGet();
    }
  }

  private static byte[] newBuffer1024(byte[] buf) {
    if (buf != null) {
      return buf;
    }
    _blurShardBuffercacheAllocate1024.incrementAndGet();
    return new byte[1024];
  }

  private static byte[] newBuffer8192(byte[] buf) {
    if (buf != null) {
      return buf;
    }
    _blurShardBuffercacheAllocate8192.incrementAndGet();
    return new byte[8192];
  }

  private static byte[] newBuffer(int size) {
    _blurShardBuffercacheAllocateOther.incrementAndGet();
    return new byte[size];
  }
}
