package org.apache.blur.utils;

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
import java.util.SortedSet;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

public class QueryCache implements EvictionListener<QueryCacheKey, QueryCacheEntry> {

  private static final Log LOG = LogFactory.getLog(QueryCache.class);

  private long _ttl;
  private String _name;
  private int _cachedElements;
  private ConcurrentLinkedHashMap<QueryCacheKey, QueryCacheEntry> _cache;

  public QueryCache(String name, int cachedElements, long ttl) {
    _name = name;
    _cachedElements = cachedElements;
    _ttl = ttl;
    _cache = new ConcurrentLinkedHashMap.Builder<QueryCacheKey, QueryCacheEntry>().maximumWeightedCapacity(_cachedElements).listener(this).build();
  }

  @Override
  public void onEviction(QueryCacheKey key, QueryCacheEntry value) {
    LOG.debug("Cache [" + _name + "] key [" + key + "] value [" + value + "] evicted.");
  }

  public static QueryCacheKey getNormalizedBlurQueryKey(String table, BlurQuery blurQuery) {
    BlurQuery newBlurQuery = new BlurQuery(blurQuery);
    newBlurQuery.useCacheIfPresent = false;
    newBlurQuery.userContext = null;
    newBlurQuery.maxQueryTime = 0;
    newBlurQuery.uuid = 0;
    newBlurQuery.startTime = 0;
    return new QueryCacheKey(table, newBlurQuery);
  }

  public boolean isValid(QueryCacheEntry entry, SortedSet<String> currentShards) {
    if (!isValid(entry)) {
      return false;
    }
    if (!entry.shards.equals(currentShards)) {
      return false;
    }
    return true;
  }

  public boolean isValid(QueryCacheEntry entry) {
    if (entry == null) {
      return false;
    }
    if (entry.timestamp + _ttl < System.currentTimeMillis()) {
      return false;
    }
    return true;
  }

  public BlurResults cache(String table, BlurQuery original, BlurResults results) {
    if (results == null) {
      return null;
    }
    if (original != null && original.cacheResult) {
      LOG.debug("Caching results for query [{0}]", original);
      BlurResults cacheResults = new BlurResults(results);
      cacheResults.query = null;
      put(getNormalizedBlurQueryKey(table, original), new QueryCacheEntry(cacheResults));
    }
    return results;
  }

  public void put(QueryCacheKey key, QueryCacheEntry value) {
    _cache.put(key, value);
  }

  public QueryCacheEntry get(QueryCacheKey key) {
    return _cache.get(key);
  }

  public void remove(QueryCacheKey key) {
    _cache.remove(key);
  }
}
