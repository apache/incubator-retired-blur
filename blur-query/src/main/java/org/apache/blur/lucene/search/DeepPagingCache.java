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
package org.apache.blur.lucene.search;

import static org.apache.blur.metrics.MetricsConstants.DEEP_PAGING_CACHE;
import static org.apache.blur.metrics.MetricsConstants.EVICTION;
import static org.apache.blur.metrics.MetricsConstants.HIT;
import static org.apache.blur.metrics.MetricsConstants.MISS;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.SIZE;

import java.lang.ref.WeakReference;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class DeepPagingCache {

  private final static Log LOG = LogFactory.getLog(DeepPagingCache.class);

  private static final long DEFAULT_MAX = 1024;

  private final ConcurrentNavigableMap<DeepPageKeyPlusPosition, DeepPageContainer> _positionCache;
  private final ConcurrentLinkedHashMap<DeepPageKeyPlusPosition, DeepPageContainer> _lruCache;
  private final Meter _hits;
  private final Meter _misses;
  private final Meter _evictions;

  public DeepPagingCache() {
    this(DEFAULT_MAX);
  }

  public DeepPagingCache(long maxEntriesForDeepPaging) {
    _hits = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, DEEP_PAGING_CACHE, HIT), HIT, TimeUnit.SECONDS);
    _misses = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, DEEP_PAGING_CACHE, MISS), MISS, TimeUnit.SECONDS);
    _evictions = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, DEEP_PAGING_CACHE, EVICTION), EVICTION,
        TimeUnit.SECONDS);
    _lruCache = new ConcurrentLinkedHashMap.Builder<DeepPageKeyPlusPosition, DeepPageContainer>()
        .maximumWeightedCapacity(maxEntriesForDeepPaging)
        .listener(new EvictionListener<DeepPageKeyPlusPosition, DeepPageContainer>() {
          @Override
          public void onEviction(DeepPageKeyPlusPosition key, DeepPageContainer value) {
            _positionCache.remove(key);
            _evictions.mark();
          }
        }).build();
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, DEEP_PAGING_CACHE, SIZE), new Gauge<Long>() {
      @Override
      public Long value() {
        return _lruCache.weightedSize();
      }
    });
    _positionCache = new ConcurrentSkipListMap<DeepPageKeyPlusPosition, DeepPageContainer>();
  }

  public void add(DeepPageKey key, DeepPageContainer deepPageContainer) {
    LOG.debug("Adding DeepPageContainer [{0}] for key [{1}]", deepPageContainer, key);
    DeepPageKeyPlusPosition k = new DeepPageKeyPlusPosition(deepPageContainer.position, key);
    _lruCache.put(k, deepPageContainer);
    _positionCache.put(k, deepPageContainer);
  }

  public DeepPageContainer lookup(DeepPageKey key, int skipTo) {
    LOG.debug("Looking up DeepPageContainer for skipTo [{0}] with key hashcode [{1}] and key [{2}]", skipTo,
        key.hashCode(), key);
    DeepPageKeyPlusPosition k = new DeepPageKeyPlusPosition(skipTo, key);
    DeepPageContainer deepPageContainer = _lruCache.get(k);
    if (deepPageContainer != null) {
      _hits.mark();
      return deepPageContainer;
    }

    ConcurrentNavigableMap<DeepPageKeyPlusPosition, DeepPageContainer> headMap = _positionCache.headMap(k, true);
    if (headMap == null || headMap.isEmpty()) {
      _misses.mark();
      return null;
    }
    Entry<DeepPageKeyPlusPosition, DeepPageContainer> firstEntry = headMap.lastEntry();
    DeepPageKeyPlusPosition dpkpp = firstEntry.getKey();
    if (dpkpp._deepPageKey.equals(key)) {
      _hits.mark();
      return firstEntry.getValue();
    }
    _misses.mark();
    return null;
  }

  static class DeepPageKeyWithPosition {
    final DeepPageKey _deepPageKey;
    final int _position;

    DeepPageKeyWithPosition(DeepPageKey deepPageKey, int position) {
      _deepPageKey = deepPageKey;
      _position = position;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((_deepPageKey == null) ? 0 : _deepPageKey.hashCode());
      result = prime * result + _position;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      DeepPageKeyWithPosition other = (DeepPageKeyWithPosition) obj;
      if (_deepPageKey == null) {
        if (other._deepPageKey != null)
          return false;
      } else if (!_deepPageKey.equals(other._deepPageKey))
        return false;
      if (_position != other._position)
        return false;
      return true;
    }

  }

  static class DeepPageKeyPlusPosition implements Comparable<DeepPageKeyPlusPosition> {

    final int _position;
    final DeepPageKey _deepPageKey;

    DeepPageKeyPlusPosition(int position, DeepPageKey deepPageKey) {
      _position = position;
      _deepPageKey = deepPageKey;
    }

    @Override
    public int compareTo(DeepPageKeyPlusPosition o) {
      int compareTo = _deepPageKey.compareTo(o._deepPageKey);
      if (compareTo == 0) {
        return _position - o._position;
      }
      return compareTo;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((_deepPageKey == null) ? 0 : _deepPageKey.hashCode());
      result = prime * result + _position;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      DeepPageKeyPlusPosition other = (DeepPageKeyPlusPosition) obj;
      if (_deepPageKey == null) {
        if (other._deepPageKey != null)
          return false;
      } else if (!_deepPageKey.equals(other._deepPageKey))
        return false;
      if (_position != other._position)
        return false;
      return true;
    }

  }

  public static class DeepPageKey implements Comparable<DeepPageKey> {

    public DeepPageKey(Query q, Sort s, Object o) {
      _indexKey = new WeakReference<Object>(o);
      _sort = s;
      _query = q;
    }

    final WeakReference<Object> _indexKey;
    final Query _query;
    final Sort _sort;

    @Override
    public int compareTo(DeepPageKey o) {
      return hashCode() - o.hashCode();
    }

    private Object getIndexKey() {
      return _indexKey.get();
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((getIndexKey() == null) ? 0 : getIndexKey().hashCode());
      result = prime * result + ((_query == null) ? 0 : _query.hashCode());
      result = prime * result + ((_sort == null) ? 0 : _sort.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      DeepPageKey other = (DeepPageKey) obj;
      if (getIndexKey() == null) {
        if (other.getIndexKey() != null)
          return false;
      } else if (!getIndexKey().equals(other.getIndexKey()))
        return false;
      if (_query == null) {
        if (other._query != null)
          return false;
      } else if (!_query.equals(other._query))
        return false;
      if (_sort == null) {
        if (other._sort != null)
          return false;
      } else if (!_sort.equals(other._sort))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "DeepPageKey [_indexKey=" + getIndexKey() + ", _query=" + _query + ", _sort=" + _sort + "]";
    }

  }

  public static class DeepPageContainer {
    int position;
    ScoreDoc scoreDoc;

    @Override
    public String toString() {
      return "DeepPageContainer [position=" + position + ", scoreDoc=" + scoreDoc + "]";
    }
  }

}
