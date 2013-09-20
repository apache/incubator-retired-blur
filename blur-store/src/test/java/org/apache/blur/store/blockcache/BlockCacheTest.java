package org.apache.blur.store.blockcache;

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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class BlockCacheTest {
  @Test
  public void testBlockCache() throws IOException, InterruptedException, ExecutionException {
    int blocksPerSlab = 1024;
    int slabs = 4;
    // Test block are larger than cache size.
    final int blocksInTest = (blocksPerSlab * slabs) * 2;
    final int blockSize = BlockCache._8K;
    int slabSize = blockSize * blocksPerSlab;
    long totalMemory = slabs * (long) slabSize;

    final BlockCache blockCache = new BlockCache(true, totalMemory, slabSize);
    final int threads = 4;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
    final int testSpace = blocksInTest / threads;
    for (int g = 0; g < threads; g++) {
      final int file = g;
      futures.add(pool.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return runTest(blockSize, file, testSpace, blockCache);
        }
      }));
    }

    for (Future<Boolean> future : futures) {
      if (!future.get()) {
        fail();
      }
    }
    blockCache.close();
  }

  private boolean runTest(int blockSize, int file, int blocksInTest, BlockCache blockCache) {
    byte[] buffer = new byte[blockSize];
    Random random = new Random();

    byte[] newData = new byte[blockSize];
    AtomicLong hitsInCache = new AtomicLong();
    AtomicLong missesInCache = new AtomicLong();
    long storeTime = 0;
    long fetchTime = 0;
    int passes = 10000;

    BlockCacheKey blockCacheKey = new BlockCacheKey();

    for (int j = 0; j < passes; j++) {
      long block = random.nextInt(blocksInTest);
      blockCacheKey.setBlock(block);
      blockCacheKey.setFile(file);

      if (blockCache.fetch(blockCacheKey, buffer)) {
        hitsInCache.incrementAndGet();
      } else {
        missesInCache.incrementAndGet();
      }

      byte[] testData = testData(random, blockSize, newData);
      long t1 = System.nanoTime();
      boolean store = blockCache.store(blockCacheKey, 0, testData, 0, blockSize);
      storeTime += (System.nanoTime() - t1);

      if (store) {
        long t3 = System.nanoTime();
        if (blockCache.fetch(blockCacheKey, buffer)) {
          fetchTime += (System.nanoTime() - t3);
          if (!Arrays.equals(testData, buffer)) {
            return false;
          }
        }
      }
    }
    System.out.println("Cache Hits    = " + hitsInCache.get());
    System.out.println("Cache Misses  = " + missesInCache.get());
    System.out.println("Store         = avg " + (storeTime / (double) passes) / 1000000.0 + " ms");
    System.out.println("Fetch         = avg " + (fetchTime / (double) passes) / 1000000.0 + " ms");
    System.out.println("# of Elements = " + blockCache.getSize());
    return true;
  }

  /**
   * Verify checking of buffer size limits against the cached block size.
   * @throws IOException 
   */
  @Test
  public void testLongBuffer() throws IOException {
    Random random = new Random();
    int blockSize = BlockCache._8K;
    int slabSize = blockSize * 1024;
    long totalMemory = 2 * slabSize;

    BlockCache blockCache = new BlockCache(true, totalMemory, slabSize);
    BlockCacheKey blockCacheKey = new BlockCacheKey();
    blockCacheKey.setBlock(0);
    blockCacheKey.setFile(0);
    byte[] newData = new byte[blockSize * 3];
    byte[] testData = testData(random, blockSize, newData);

    assertTrue(blockCache.store(blockCacheKey, 0, testData, 0, blockSize));
    assertTrue(blockCache.store(blockCacheKey, 0, testData, blockSize, blockSize));
    assertTrue(blockCache.store(blockCacheKey, 0, testData, blockSize * 2, blockSize));

    assertTrue(blockCache.store(blockCacheKey, 1, testData, 0, blockSize - 1));
    assertTrue(blockCache.store(blockCacheKey, 1, testData, blockSize, blockSize - 1));
    assertTrue(blockCache.store(blockCacheKey, 1, testData, blockSize * 2, blockSize - 1));
    blockCache.close(); 
  }

  private static byte[] testData(Random random, int size, byte[] buf) {
    random.nextBytes(buf);
    return buf;
  }
}
