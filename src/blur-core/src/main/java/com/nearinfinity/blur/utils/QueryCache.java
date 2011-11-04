package com.nearinfinity.blur.utils;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResults;

public class QueryCache extends SimpleLRUCache<BlurQuery, QueryCacheEntry> {

  private static final Log LOG = LogFactory.getLog(QueryCache.class);

  private static final long serialVersionUID = -7314843147288776095L;
  private long _ttl;

  public QueryCache(String name, int cachedElements, long ttl) {
    super(name, cachedElements);
    _ttl = ttl;
  }

  public BlurQuery getNormalizedBlurQuery(BlurQuery blurQuery) {
    BlurQuery newBlurQuery = new BlurQuery(blurQuery);
    newBlurQuery.allowStaleData = false;
    newBlurQuery.useCacheIfPresent = false;
    newBlurQuery.userContext = null;
    newBlurQuery.maxQueryTime = 0;
    newBlurQuery.uuid = 0;
    newBlurQuery.startTime = 0;
    newBlurQuery.modifyFileCaches = false;
    return newBlurQuery;
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

  public BlurResults cache(BlurQuery original, BlurResults results) {
    if (results == null) {
      return null;
    }
    if (original != null && original.cacheResult) {
      LOG.debug("Caching results for query [{0}]", original);
      BlurResults cacheResults = new BlurResults(results);
      cacheResults.query = null;
      put(getNormalizedBlurQuery(original), new QueryCacheEntry(cacheResults));
    }
    return results;
  }
}
