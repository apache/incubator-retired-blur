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

import static org.apache.blur.metrics.MetricsConstants.COUNT;
import static org.apache.blur.metrics.MetricsConstants.EVICTION;
import static org.apache.blur.metrics.MetricsConstants.HIT;
import static org.apache.blur.metrics.MetricsConstants.MISS;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.SIZE;
import static org.apache.blur.metrics.MetricsConstants.THRIFT_CACHE;
import static org.apache.blur.metrics.MetricsConstants.THRIFT_CACHE_ATTRIBUTE_MAP;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TBase;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;

import com.google.common.collect.MapMaker;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EntryWeigher;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class ThriftCache {

  private static final String KEY = "key";
  private static final String CLASS = "class";
  private static final String T_KEY = "tKey";
  private static final String SHARDS = "shards";
  private static final String TABLE = "table";
  private static final String THRIFT_CACHE_PUT = "ThriftCache - put";
  private static final String THRIFT_CACHE_GET = "ThriftCache - get";
  private static final String THRIFT_CACHE_GET_KEY = "ThriftCache - getKey";

  private static final Log LOG = LogFactory.getLog(ThriftCache.class);

  private final ConcurrentLinkedHashMap<ThriftCacheKey<?>, ThriftCacheValue<?>> _cacheMap;
  private final ConcurrentMap<Map<String, String>, Map<String, String>> _attributeKeys;
  private final ConcurrentMap<ShardsKey, ShardsKey> _shardsKeys;
  private final ConcurrentMap<String, ClassObj<?>> _classObjMap = new ConcurrentHashMap<String, ClassObj<?>>();
  private final ConcurrentMap<String, Long> _lastModTimestamps = new ConcurrentHashMap<String, Long>();
  private final Meter _hits;
  private final Meter _misses;
  private final Meter _evictions;
  private final AtomicLong _hitsAtomicLong;
  private final AtomicLong _missesAtomicLong;
  private final AtomicLong _evictionsAtomicLong;

  public ThriftCache(long totalNumberOfBytes) {
    _attributeKeys = new MapMaker().weakKeys().makeMap();
    _shardsKeys = new MapMaker().weakKeys().makeMap();
    _hits = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, THRIFT_CACHE, HIT), HIT, TimeUnit.SECONDS);
    _misses = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, THRIFT_CACHE, MISS), MISS, TimeUnit.SECONDS);
    _evictions = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, THRIFT_CACHE, EVICTION), EVICTION, TimeUnit.SECONDS);
    _cacheMap = new ConcurrentLinkedHashMap.Builder<ThriftCacheKey<?>, ThriftCacheValue<?>>()
        .weigher(new EntryWeigher<ThriftCacheKey<?>, ThriftCacheValue<?>>() {
          @Override
          public int weightOf(ThriftCacheKey<?> key, ThriftCacheValue<?> value) {
            return key.size() + value.size();
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
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, THRIFT_CACHE, SIZE), new Gauge<Long>() {
      @Override
      public Long value() {
        return _cacheMap.weightedSize();
      }
    });
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, THRIFT_CACHE, COUNT), new Gauge<Long>() {
      @Override
      public Long value() {
        return (long) _cacheMap.size();
      }
    });
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, THRIFT_CACHE_ATTRIBUTE_MAP, COUNT), new Gauge<Long>() {
      @Override
      public Long value() {
        return (long) _attributeKeys.size();
      }
    });
  }

  public <K extends TBase<?, ?>, V extends TBase<?, ?>> V put(ThriftCacheKey<K> key, V t) throws BlurException {
    Tracer trace = Trace.trace(THRIFT_CACHE_PUT, Trace.param(KEY, key));
    try {
      synchronized (_lastModTimestamps) {
        Long lastModTimestamp = _lastModTimestamps.get(key.getTable());
        if (lastModTimestamp != null && key.getTimestamp() < lastModTimestamp) {
          // This means that the key was created before the index was modified.
          // So do not cache the value because it's already out of date with the
          // index.
          return t;
        }
      }
      LOG.debug("Inserting into cache [{0}] with key [{1}]", t, key);
      _cacheMap.put(key, new ThriftCacheValue<V>(t));
      return t;
    } finally {
      trace.done();
    }
  }

  @SuppressWarnings("unchecked")
  public <K extends TBase<?, ?>, V extends TBase<?, ?>> V get(ThriftCacheKey<K> key, Class<V> clazz)
      throws BlurException {
    Tracer trace = Trace.trace(THRIFT_CACHE_GET, Trace.param(KEY, key));
    try {
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
    } finally {
      trace.done();
    }
  }

  public <K extends TBase<?, ?>> ThriftCacheKey<K> getKey(String table, int[] shards, K tkey, Class<K> clazz)
      throws BlurException {
    Tracer trace = Trace.trace(THRIFT_CACHE_GET_KEY, Trace.param(TABLE, table), Trace.param(SHARDS, shards),
        Trace.param(T_KEY, tkey), Trace.param(CLASS, clazz));
    try {
      User user = UserContext.getUser();
      Map<String, String> userAttributes = getUserAttributes(user);
      ClassObj<K> classObj = getClassObj(clazz);
      ShardsKey shardsKey = getShardsKey(shards);
      return new ThriftCacheKey<K>(userAttributes, table, shardsKey, tkey, classObj);
    } finally {
      trace.done();
    }
  }

  private ShardsKey getShardsKey(int[] shards) {
    ShardsKey shardsKey = new ShardsKey(shards);
    ShardsKey insternalShardsKey = _shardsKeys.get(shardsKey);
    if (insternalShardsKey != null) {
      return insternalShardsKey;
    }
    insternalShardsKey = _shardsKeys.putIfAbsent(shardsKey, shardsKey);
    if (insternalShardsKey == null) {
      return shardsKey;
    } else {
      return insternalShardsKey;
    }
  }

  @SuppressWarnings("unchecked")
  private <T> ClassObj<T> getClassObj(Class<T> clazz) {
    String name = clazz.getName();
    ClassObj<T> classObj = (ClassObj<T>) _classObjMap.get(name);
    if (classObj != null) {
      return classObj;
    }
    classObj = new ClassObj<T>(clazz);
    ClassObj<T> currentValue = (ClassObj<T>) _classObjMap.putIfAbsent(name, classObj);
    if (currentValue == null) {
      return classObj;
    }
    return currentValue;
  }

  private Map<String, String> getUserAttributes(User user) {
    if (user == null) {
      return null;
    }
    Map<String, String> attributes = user.getAttributes();
    if (attributes == null) {
      return null;
    }
    if (!(attributes instanceof TreeMap)) {
      attributes = new TreeMap<String, String>(attributes);
    }
    Map<String, String> internalInstance = _attributeKeys.get(attributes);
    if (internalInstance == null) {
      internalInstance = _attributeKeys.putIfAbsent(attributes, attributes);
      if (internalInstance == null) {
        return attributes;
      }
    }
    return internalInstance;
  }

  public void clearTable(String table) {
    synchronized (_lastModTimestamps) {
      _lastModTimestamps.put(table, System.nanoTime());
    }
    LOG.debug("Clearing cache for table [{0}]", table);
    Set<Entry<ThriftCacheKey<?>, ThriftCacheValue<?>>> entrySet = _cacheMap.entrySet();
    Iterator<Entry<ThriftCacheKey<?>, ThriftCacheValue<?>>> iterator = entrySet.iterator();
    while (iterator.hasNext()) {
      Entry<ThriftCacheKey<?>, ThriftCacheValue<?>> entry = iterator.next();
      if (entry.getKey().getTable().equals(table)) {
        iterator.remove();
      }
    }
  }

  public void clear() {
    LOG.debug("Clearing all cache.");
    _cacheMap.clear();
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
