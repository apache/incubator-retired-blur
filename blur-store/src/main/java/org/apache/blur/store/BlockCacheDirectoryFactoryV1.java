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

import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCKCACHE_DIRECT_MEMORY_ALLOCATION;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCKCACHE_SLAB_COUNT;

import java.io.IOException;
import java.util.Set;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.blockcache.BlockCache;
import org.apache.blur.store.blockcache.BlockDirectory;
import org.apache.blur.store.blockcache.BlockDirectoryCache;
import org.apache.blur.store.blockcache.Cache;
import org.apache.lucene.store.Directory;

public class BlockCacheDirectoryFactoryV1 extends BlockCacheDirectoryFactory {

  private static final Log LOG = LogFactory.getLog(BlockCacheDirectoryFactoryV1.class);

  private final Cache _cache;

  public BlockCacheDirectoryFactoryV1(BlurConfiguration configuration, long totalNumberOfBytes) {
    // setup block cache
    // 134,217,728 is the slab size, therefore there are 16,384 blocks
    // in a slab when using a block size of 8,192
    int numberOfBlocksPerSlab = 16384;
    int blockSize = BlockDirectory.BLOCK_SIZE;
    int slabCount = configuration.getInt(BLUR_SHARD_BLOCKCACHE_SLAB_COUNT, -1);
    slabCount = getSlabCount(slabCount, numberOfBlocksPerSlab, blockSize, totalNumberOfBytes);
    Cache cache;
    if (slabCount >= 1) {
      BlockCache blockCache;
      boolean directAllocation = configuration.getBoolean(BLUR_SHARD_BLOCKCACHE_DIRECT_MEMORY_ALLOCATION, true);

      int slabSize = numberOfBlocksPerSlab * blockSize;
      LOG.info("Number of slabs of block cache [{0}] with direct memory allocation set to [{1}]", slabCount,
          directAllocation);
      LOG.info("Block cache target memory usage, slab size of [{0}] will allocate [{1}] slabs and use ~[{2}] bytes",
          slabSize, slabCount, ((long) slabCount * (long) slabSize));

      try {
        long totalMemory = (long) slabCount * (long) numberOfBlocksPerSlab * (long) blockSize;
        blockCache = new BlockCache(directAllocation, totalMemory, slabSize);
      } catch (OutOfMemoryError e) {
        if ("Direct buffer memory".equals(e.getMessage())) {
          System.err
              .println("The max direct memory is too low.  Either increase by setting (-XX:MaxDirectMemorySize=<size>g -XX:+UseLargePages) or disable direct allocation by (blur.shard.blockcache.direct.memory.allocation=false) in blur-site.properties");
          System.exit(1);
        }
        throw e;
      }
      cache = new BlockDirectoryCache(blockCache);
    } else {
      cache = BlockDirectory.NO_CACHE;
    }
    _cache = cache;
  }

  @Override
  public Directory newDirectory(String table, String shard, Directory directory, Set<String> blockCacheFileTypes)
      throws IOException {
    return new BlockDirectory(table + "_" + shard, directory, _cache, blockCacheFileTypes);
  }

  private static int getSlabCount(int slabCount, int numberOfBlocksPerSlab, int blockSize, long totalNumberOfBytes) {
    if (slabCount < 0) {
      long slabSize = numberOfBlocksPerSlab * blockSize;
      if (totalNumberOfBytes < slabSize) {
        throw new RuntimeException("Auto slab setup cannot happen, JVM option -XX:MaxDirectMemorySize not set.");
      }
      return (int) (totalNumberOfBytes / slabSize);
    }
    return slabCount;
  }

  @Override
  public void close() throws IOException {

  }

}
