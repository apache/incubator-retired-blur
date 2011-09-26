package com.nearinfinity.blur.store.blockcache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockDirectoryCache implements Cache {
  private BlockCache blockCache;
  private AtomicInteger counter = new AtomicInteger();
  private Map<String,Integer> names = new ConcurrentHashMap<String, Integer>();
  
  public BlockDirectoryCache(BlockCache blockCache) {
    this.blockCache = blockCache;
  }
  
  @Override
  public void delete(String name) {
    names.remove(name);
  }
  
  @Override
  public void update(String name, long blockId, byte[] buffer) {
    Integer file = names.get(name);
    if (file == null) {
      file = counter.incrementAndGet();
      names.put(name, file);
    }
    BlockCacheKey blockCacheKey = new BlockCacheKey();
    blockCacheKey.setBlock(blockId);
    blockCacheKey.setFile(file);
    synchronized (blockCache) {
      blockCache.store(blockCacheKey, buffer);  
    }
  }

  @Override
  public boolean fetch(String name, long blockId, int blockOffset, byte[] b, int off, int lengthToReadInBlock) {
    Integer file = names.get(name);
    if (file == null) {
      return false;
    }
    BlockCacheKey blockCacheKey = new BlockCacheKey();
    blockCacheKey.setBlock(blockId);
    blockCacheKey.setFile(file);
    synchronized (blockCache) {
      return blockCache.fetch(blockCacheKey, b, blockOffset, off, lengthToReadInBlock);
    }
  }
}
