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
package org.apache.blur.server.cache;

import static org.apache.blur.metrics.MetricsConstants.EVICTION;
import static org.apache.blur.metrics.MetricsConstants.HIT;
import static org.apache.blur.metrics.MetricsConstants.MISS;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.THRIFT_CACHE;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TBase;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class ThriftCache {

  private static final Log LOG = LogFactory.getLog(ThriftCache.class);

  private final ConcurrentLinkedHashMap<ThriftCacheKey<?>, ThriftCacheValue<?>> _cacheMap;
  private final Meter _hits;
  private final Meter _misses;
  private final Meter _evictions;
  private final AtomicLong _hitsAtomicLong;
  private final AtomicLong _missesAtomicLong;
  private final AtomicLong _evictionsAtomicLong;

  public ThriftCache(long totalNumberOfBytes) {
    _hits = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, THRIFT_CACHE, HIT), HIT, TimeUnit.SECONDS);
    _misses = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, THRIFT_CACHE, MISS), MISS, TimeUnit.SECONDS);
    _evictions = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, THRIFT_CACHE, EVICTION), EVICTION, TimeUnit.SECONDS);
    _cacheMap = new ConcurrentLinkedHashMap.Builder<ThriftCacheKey<?>, ThriftCacheValue<?>>()
        .weigher(new Weigher<ThriftCacheValue<?>>() {
          @Override
          public int weightOf(ThriftCacheValue<?> value) {
            return value.size();
          }
        }).listener(new EvictionListener<ThriftCacheKey<?>, ThriftCacheValue<?>>() {
          @Override
          public void onEviction(ThriftCacheKey<?> key, ThriftCacheValue<?> value) {
            _evictions.mark();
            _evictionsAtomicLong.incrementAndGet();
          }
        }).maximumWeightedCapacity(totalNumberOfBytes).build();
    _hitsAtomicLong = new AtomicLong();
    _missesAtomicLong = new AtomicLong();
    _evictionsAtomicLong = new AtomicLong();
  }

  public <K extends TBase<?, ?>, V extends TBase<?, ?>> V put(ThriftCacheKey<K> key, V t) throws BlurException {
    _cacheMap.put(key, new ThriftCacheValue<V>(t));
    return t;
  }

  @SuppressWarnings("unchecked")
  public <K extends TBase<?, ?>, V extends TBase<?, ?>> V get(ThriftCacheKey<K> key, Class<V> clazz)
      throws BlurException {
    ThriftCacheValue<V> value = (ThriftCacheValue<V>) _cacheMap.get(key);
    if (value == null) {
      LOG.debug("Cache Miss for [{0}]", key);
      _misses.mark();
      _missesAtomicLong.incrementAndGet();
      return null;
    }
    LOG.debug("Cache Hit for [{0}]", key);
    _hits.mark();
    _hitsAtomicLong.incrementAndGet();
    return value.getValue(clazz);
  }

  public <K extends TBase<?, ?>> ThriftCacheKey<K> getKey(String table, K tkey, Class<K> clazz) {
    User user = UserContext.getUser();
    return new ThriftCacheKey<K>(user, table, tkey, clazz);
  }

  public void clear() {
    LOG.info("Clearing all cache.");
    _cacheMap.clear();
  }

  public void clearTable(String table) {
    LOG.info("Clearing cache for table [{0}]", table);
    Set<Entry<ThriftCacheKey<?>, ThriftCacheValue<?>>> entrySet = _cacheMap.entrySet();
    Iterator<Entry<ThriftCacheKey<?>, ThriftCacheValue<?>>> iterator = entrySet.iterator();
    while (iterator.hasNext()) {
      Entry<ThriftCacheKey<?>, ThriftCacheValue<?>> entry = iterator.next();
      if (entry.getKey().getTable().equals(table)) {
        iterator.remove();
      }
    }
  }

  public long size() {
    return _cacheMap.weightedSize();
  }

  public long getHits() {
    return _hitsAtomicLong.get();
  }

  public long getMisses() {
    return _missesAtomicLong.get();
  }

  public long getEvictions() {
    return _evictionsAtomicLong.get();
  }

}
