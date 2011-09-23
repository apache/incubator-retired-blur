package com.nearinfinity.blur.store.blockcache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockDirectoryCache implements DirectoryCache {
  private BlockCache blockCache;
  private AtomicInteger counter = new AtomicInteger();
  private Map<String,Integer> names = new HashMap<String, Integer>();
  
  public BlockDirectoryCache(BlockCache blockCache) {
    this.blockCache = blockCache;
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
    blockCache.store(blockCacheKey, buffer);
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
    return blockCache.fetch(blockCacheKey, b, blockOffset, off, lengthToReadInBlock);
  }
}
