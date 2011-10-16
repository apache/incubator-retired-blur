package com.nearinfinity.blur.store.blockcache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.nearinfinity.blur.metrics.BlurMetrics;

public class BlockDirectoryCache implements Cache {
  private BlockCache _blockCache;
  private AtomicInteger _counter = new AtomicInteger();
  private Map<String,Integer> _names = new ConcurrentHashMap<String, Integer>();
  private BlurMetrics _blurMetrics;
  
  public BlockDirectoryCache(BlockCache blockCache, BlurMetrics blurMetrics) {
    _blockCache = blockCache;
    _blurMetrics = blurMetrics;
  }
  
  @Override
  public void delete(String name) {
    _names.remove(name);
  }
  
  @Override
  public void update(String name, long blockId, byte[] buffer) {
    Integer file = _names.get(name);
    if (file == null) {
      file = _counter.incrementAndGet();
      _names.put(name, file);
    }
    BlockCacheKey blockCacheKey = new BlockCacheKey();
    blockCacheKey.setBlock(blockId);
    blockCacheKey.setFile(file);
    _blockCache.store(blockCacheKey, buffer);  
  }

  @Override
  public boolean fetch(String name, long blockId, int blockOffset, byte[] b, int off, int lengthToReadInBlock) {
    Integer file = _names.get(name);
    if (file == null) {
      return false;
    }
    BlockCacheKey blockCacheKey = new BlockCacheKey();
    blockCacheKey.setBlock(blockId);
    blockCacheKey.setFile(file);
    boolean fetch = _blockCache.fetch(blockCacheKey, b, blockOffset, off, lengthToReadInBlock);
    if (fetch) {
      _blurMetrics.hdfsCacheHit.inc();
    } else {
      _blurMetrics.hdfsCacheMiss.inc();
    }
    return fetch;
  }
}
