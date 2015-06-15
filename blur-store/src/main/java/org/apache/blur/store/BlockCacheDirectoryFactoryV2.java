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
package org.apache.blur.store;

import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE_PREFIX;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_FILE_BUFFER_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_POOL_CACHE_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_READ_CACHE_EXT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_READ_DEFAULT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_READ_NOCACHE_EXT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_SLAB_CHUNK_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_SLAB_ENABLED;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_SLAB_SIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_STORE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_WRITE_CACHE_EXT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_WRITE_DEFAULT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCK_CACHE_V2_WRITE_NOCACHE_EXT;
import static org.apache.blur.utils.BlurConstants.DEFAULT_VALUE;
import static org.apache.blur.utils.BlurConstants.OFF_HEAP;
import static org.apache.blur.utils.BlurConstants.SHARED_MERGE_SCHEDULER_PREFIX;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.blockcache_v2.BaseCache;
import org.apache.blur.store.blockcache_v2.BaseCache.STORE;
import org.apache.blur.store.blockcache_v2.BaseCacheValueBufferPool;
import org.apache.blur.store.blockcache_v2.Cache;
import org.apache.blur.store.blockcache_v2.CacheDirectory;
import org.apache.blur.store.blockcache_v2.CachePoolStrategy;
import org.apache.blur.store.blockcache_v2.FileNameFilter;
import org.apache.blur.store.blockcache_v2.PooledCache;
import org.apache.blur.store.blockcache_v2.Quiet;
import org.apache.blur.store.blockcache_v2.SimpleCacheValueBufferPool;
import org.apache.blur.store.blockcache_v2.SingleCachePoolStrategy;
import org.apache.blur.store.blockcache_v2.Size;
import org.apache.blur.store.blockcache_v2.SlabAllocationCacheValueBufferPool;
import org.apache.lucene.store.Directory;

public class BlockCacheDirectoryFactoryV2 extends BlockCacheDirectoryFactory {



  private static final Log LOG = LogFactory.getLog(BlockCacheDirectoryFactoryV2.class);

  private final Cache _cache;

  public BlockCacheDirectoryFactoryV2(BlurConfiguration configuration, long totalNumberOfBytes) {

    final int fileBufferSizeInt = configuration.getInt(BLUR_SHARD_BLOCK_CACHE_V2_FILE_BUFFER_SIZE, 8192);
    LOG.info("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_FILE_BUFFER_SIZE, fileBufferSizeInt);
    final int cacheBlockSizeInt = configuration.getInt(BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE, 8192);
    LOG.info("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE, cacheBlockSizeInt);

    final Map<String, Integer> cacheBlockSizeMap = new HashMap<String, Integer>();
    Map<String, String> properties = configuration.getProperties();
    for (Entry<String, String> prop : properties.entrySet()) {
      String key = prop.getKey();
      String value = prop.getValue();
      if (value == null || value.isEmpty()) {
        continue;
      }
      if (key.startsWith(BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE_PREFIX)) {
        int cacheBlockSizeForFile = Integer.parseInt(value);
        String fieldType = key.substring(BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE_PREFIX.length());

        cacheBlockSizeMap.put(fieldType, cacheBlockSizeForFile);
        LOG.info("{0}={1} for file type [{2}]", key, cacheBlockSizeForFile, fieldType);
      }
    }

    final STORE store = STORE.valueOf(configuration.get(BLUR_SHARD_BLOCK_CACHE_V2_STORE, OFF_HEAP));
    LOG.info("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_STORE, store);

    final Set<String> cachingFileExtensionsForRead = getSet(configuration.get(BLUR_SHARD_BLOCK_CACHE_V2_READ_CACHE_EXT,
        DEFAULT_VALUE));
    LOG.info("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_READ_CACHE_EXT, cachingFileExtensionsForRead);

    final Set<String> nonCachingFileExtensionsForRead = getSet(configuration.get(
        BLUR_SHARD_BLOCK_CACHE_V2_READ_NOCACHE_EXT, DEFAULT_VALUE));
    LOG.info("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_READ_NOCACHE_EXT, nonCachingFileExtensionsForRead);

    final boolean defaultReadCaching = configuration.getBoolean(BLUR_SHARD_BLOCK_CACHE_V2_READ_DEFAULT, true);
    LOG.info("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_READ_DEFAULT, defaultReadCaching);

    final Set<String> cachingFileExtensionsForWrite = getSet(configuration.get(
        BLUR_SHARD_BLOCK_CACHE_V2_WRITE_CACHE_EXT, DEFAULT_VALUE));
    LOG.info("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_WRITE_CACHE_EXT, cachingFileExtensionsForWrite);

    final Set<String> nonCachingFileExtensionsForWrite = getSet(configuration.get(
        BLUR_SHARD_BLOCK_CACHE_V2_WRITE_NOCACHE_EXT, DEFAULT_VALUE));
    LOG.info("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_WRITE_NOCACHE_EXT, nonCachingFileExtensionsForWrite);

    final boolean defaultWriteCaching = configuration.getBoolean(BLUR_SHARD_BLOCK_CACHE_V2_WRITE_DEFAULT, true);
    LOG.info("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_WRITE_DEFAULT, defaultWriteCaching);

    Size fileBufferSize = new Size() {
      @Override
      public int getSize(CacheDirectory directory, String fileName) {
        return fileBufferSizeInt;
      }
    };

    Size cacheBlockSize = new Size() {
      @Override
      public int getSize(CacheDirectory directory, String fileName) {
        String ext = getExt(fileName);
        Integer size = cacheBlockSizeMap.get(ext);
        if (size != null) {
          return size;
        }
        return cacheBlockSizeInt;
      }
    };

    FileNameFilter readFilter = new FileNameFilter() {
      @Override
      public boolean accept(CacheDirectory directory, String fileName) {
        String ext = getExt(fileName);
        if (cachingFileExtensionsForRead.contains(ext)) {
          return true;
        } else if (nonCachingFileExtensionsForRead.contains(ext)) {
          return false;
        }
        return defaultReadCaching;
      }
    };

    FileNameFilter writeFilter = new FileNameFilter() {
      @Override
      public boolean accept(CacheDirectory directory, String fileName) {
        String ext = getExt(fileName);
        if (cachingFileExtensionsForWrite.contains(ext)) {
          return true;
        } else if (nonCachingFileExtensionsForWrite.contains(ext)) {
          return false;
        }
        return defaultWriteCaching;
      }
    };

    Quiet quiet = new Quiet() {
      @Override
      public boolean shouldBeQuiet(CacheDirectory directory, String fileName) {
        Thread thread = Thread.currentThread();
        String name = thread.getName();
        if (name.startsWith(SHARED_MERGE_SCHEDULER_PREFIX)) {
          return true;
        }
        return false;
      }
    };

    BaseCacheValueBufferPool pool;
    if (configuration.getBoolean(BLUR_SHARD_BLOCK_CACHE_V2_SLAB_ENABLED, true)) {
      int slabSize = configuration.getInt(BLUR_SHARD_BLOCK_CACHE_V2_SLAB_SIZE, 134217728);
      int chunkSize = configuration.getInt(BLUR_SHARD_BLOCK_CACHE_V2_SLAB_CHUNK_SIZE, 8192);
      pool = new SlabAllocationCacheValueBufferPool(chunkSize, slabSize);
    } else {
      int queueDepth = configuration.getInt(BLUR_SHARD_BLOCK_CACHE_V2_POOL_CACHE_SIZE, 10000);
      pool = new SimpleCacheValueBufferPool(store, queueDepth);
    }

    BaseCache baseCache = new BaseCache(totalNumberOfBytes, fileBufferSize, cacheBlockSize, readFilter, writeFilter,
        quiet, pool);
    CachePoolStrategy cachePoolStrategy = new SingleCachePoolStrategy(baseCache);
    _cache = new PooledCache(cachePoolStrategy);
  }

  private Set<String> getSet(String value) {
    String[] split = value.split(",");
    return new HashSet<String>(Arrays.asList(split));
  }

  @Override
  public Directory newDirectory(String table, String shard, Directory directory, Set<String> tableBlockCacheFileTypes)
      throws IOException {
    return new CacheDirectory(table, shard, directory, _cache, tableBlockCacheFileTypes);
  }

  private static String getExt(String fileName) {
    int indexOf = fileName.lastIndexOf('.');
    if (indexOf < 0) {
      return DEFAULT_VALUE;
    }
    return fileName.substring(indexOf + 1);
  }

  @Override
  public void close() throws IOException {
    _cache.close();
  }

  public Cache getCache() {
    return _cache;
  }

}
