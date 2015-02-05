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
package org.apache.blur.store.blockcache_v2;

import static org.apache.blur.metrics.MetricsConstants.CACHE_POOL;
import static org.apache.blur.metrics.MetricsConstants.CREATED;
import static org.apache.blur.metrics.MetricsConstants.DESTROYED;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.REUSED;

import java.io.Closeable;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.blockcache_v2.BaseCache.STORE;
import org.apache.blur.store.blockcache_v2.cachevalue.ByteArrayCacheValue;
import org.apache.blur.store.blockcache_v2.cachevalue.UnsafeCacheValue;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class CacheValueBufferPool implements Closeable {

  private static final Log LOG = LogFactory.getLog(CacheValueBufferPool.class);

  private final STORE _store;
  private final ConcurrentMap<Integer, BlockingQueue<CacheValue>> _cacheValuePool = new ConcurrentHashMap<Integer, BlockingQueue<CacheValue>>();
  private final int _capacity;
  private final Meter _reused;
  private final Meter _detroyed;
  private final Meter _created;

  public CacheValueBufferPool(STORE store, int capacity) {
    _store = store;
    _capacity = capacity;
    _created = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE_POOL, CREATED), CREATED, TimeUnit.SECONDS);
    _reused = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE_POOL, REUSED), REUSED, TimeUnit.SECONDS);
    _detroyed = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE_POOL, DESTROYED), DESTROYED, TimeUnit.SECONDS);
  }

  public CacheValue getCacheValue(int cacheBlockSize) {
    BlockingQueue<CacheValue> blockingQueue = getPool(cacheBlockSize);
    CacheValue cacheValue = blockingQueue.poll();
    if (cacheValue == null) {
      _created.mark();
      return createCacheValue(cacheBlockSize);
    }
    _reused.mark();
    return cacheValue;
  }

  private BlockingQueue<CacheValue> getPool(int cacheBlockSize) {
    BlockingQueue<CacheValue> blockingQueue = _cacheValuePool.get(cacheBlockSize);
    if (blockingQueue == null) {
      blockingQueue = buildNewBlockQueue(cacheBlockSize);
    }
    return blockingQueue;
  }

  private BlockingQueue<CacheValue> buildNewBlockQueue(int cacheBlockSize) {
    LOG.info("Allocating new ArrayBlockingQueue with capacity [{0}]", _capacity);
    BlockingQueue<CacheValue> value = new ArrayBlockingQueue<CacheValue>(_capacity);
    _cacheValuePool.putIfAbsent(cacheBlockSize, value);
    return _cacheValuePool.get(cacheBlockSize);
  }

  private CacheValue createCacheValue(int cacheBlockSize) {
    switch (_store) {
    case ON_HEAP:
      return new ByteArrayCacheValue(cacheBlockSize);
    case OFF_HEAP:
      return new UnsafeCacheValue(cacheBlockSize);
    default:
      throw new RuntimeException("Unknown type [" + _store + "]");
    }
  }

  public void returnToPool(CacheValue cacheValue) {
    if (cacheValue == null || cacheValue.isEvicted()) {
      return;
    }
    try {
      BlockingQueue<CacheValue> blockingQueue = getPool(cacheValue.length());
      if (!blockingQueue.offer(cacheValue)) {
        _detroyed.mark();
        cacheValue.release();
      }
    } catch (EvictionException e) {
      return;
    }
  }

  @Override
  public void close() {
    for (Entry<Integer, BlockingQueue<CacheValue>> e : _cacheValuePool.entrySet()) {
      BlockingQueue<CacheValue> queue = _cacheValuePool.remove(e.getKey());
      for (CacheValue cacheValue : queue) {
        cacheValue.release();
      }
    }
  }
}
