package com.nearinfinity.blur.utils;

import com.nearinfinity.blur.thrift.generated.BlurQuery;

public class QueryCache extends SimpleLRUCache<BlurQuery, QueryCacheEntry> {

    private static final long serialVersionUID = -7314843147288776095L;
    private long _ttl;
    
    public QueryCache(String name, int cachedElements, long ttl) {
        super(name, cachedElements);
        _ttl = ttl;
    }
    
    public BlurQuery getNoralizedBlurQuery(BlurQuery blurQuery) {
        BlurQuery newBlurQuery = new BlurQuery(blurQuery);
        newBlurQuery.userContext = null;
        newBlurQuery.allowStaleData = false;
        newBlurQuery.userContext = null;
        newBlurQuery.uuid = 0;
        newBlurQuery.maxQueryTime = 0;
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
}
