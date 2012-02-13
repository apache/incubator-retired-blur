package com.nearinfinity.blur.utils;

import java.util.SortedSet;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResults;

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
    newBlurQuery.allowStaleData = false;
    newBlurQuery.useCacheIfPresent = false;
    newBlurQuery.userContext = null;
    newBlurQuery.maxQueryTime = 0;
    newBlurQuery.uuid = 0;
    newBlurQuery.startTime = 0;
    newBlurQuery.modifyFileCaches = false;
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
