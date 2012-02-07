package com.nearinfinity.blur.store.blockcache;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.nearinfinity.blur.metrics.BlurMetrics;

public class BlockCacheTest {
  @Test
  public void testBlockCache() {
    int blocksInTest = 2000000;
    int blockSize = 1024;
    
    int slabSize = blockSize * 4096;
    long totalMemory = 2 * slabSize;
    
    BlockCache blockCache = new BlockCache(new BlurMetrics(new Configuration()), true,totalMemory,slabSize,blockSize);
    byte[] buffer = new byte[1024];
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
      int file = 0;
      blockCacheKey.setBlock(block);
      blockCacheKey.setFile(file);

      if (blockCache.fetch(blockCacheKey, buffer)) {
        hitsInCache.incrementAndGet();
      } else {
        missesInCache.incrementAndGet();
      }

      byte[] testData = testData(random, blockSize, newData);
      long t1 = System.nanoTime();
      blockCache.store(blockCacheKey, testData);
      storeTime += (System.nanoTime() - t1);

      long t3 = System.nanoTime();
      if (blockCache.fetch(blockCacheKey, buffer)) {
        fetchTime += (System.nanoTime() - t3);
        assertTrue(Arrays.equals(testData, buffer));
      }
    }
    System.out.println("Cache Hits    = " + hitsInCache.get());
    System.out.println("Cache Misses  = " + missesInCache.get());
    System.out.println("Store         = " + (storeTime / (double) passes) / 1000000.0);
    System.out.println("Fetch         = " + (fetchTime / (double) passes) / 1000000.0);
    System.out.println("# of Elements = " + blockCache.getSize());
  }

  private static byte[] testData(Random random, int size, byte[] buf) {
    random.nextBytes(buf);
    return buf;
  }
}
