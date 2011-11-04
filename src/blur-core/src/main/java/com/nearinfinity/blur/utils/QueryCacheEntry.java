package com.nearinfinity.blur.utils;

import java.util.SortedSet;
import java.util.TreeSet;

import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResults;

public class QueryCacheEntry {
  public BlurResults results;
  public long timestamp;
  public SortedSet<String> shards;

  public QueryCacheEntry(BlurResults cacheResults) {
    results = cacheResults;
    timestamp = System.currentTimeMillis();
    if (cacheResults != null && cacheResults.shardInfo != null) {
      shards = new TreeSet<String>(cacheResults.shardInfo.keySet());
    } else {
      shards = new TreeSet<String>();
    }
  }

  public BlurResults getBlurResults(BlurQuery blurQuery) {
    BlurResults blurResults = new BlurResults(results);
    blurResults.query = blurQuery;
    return blurResults;
  }
}
