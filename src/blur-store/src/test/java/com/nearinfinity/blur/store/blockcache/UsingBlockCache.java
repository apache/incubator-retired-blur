package com.nearinfinity.blur.store.blockcache;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;

import com.nearinfinity.blur.metrics.BlurMetrics;

public class UsingBlockCache {
  public static void main(String[] args) {

    int numberOfBlocksPerBank = 1048576;
    int blocksInTest = 2000000;
    int blockSize = 1024;
    BlockCache blockCache = new BlockCache(8, numberOfBlocksPerBank / 8, blockSize, new BlurMetrics(new Configuration()));
    byte[] buffer = new byte[1024];
    Random random = new Random();

    int i = 0;
    int numberOfFiles = 3;

    byte[] newData = new byte[blockSize];
    while (true) {
      AtomicLong hitsInCache = new AtomicLong();
      AtomicLong missesInCache = new AtomicLong();
      long storeTime = 0;
      long fetchTime = 0;
      int passes = 1000000;

      BlockCacheKey blockCacheKey = new BlockCacheKey();

      for (int j = 0; j < passes; j++) {
        long block = random.nextInt(blocksInTest);
        // int file = random.nextInt(numberOfFiles);
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
          if (!Arrays.equals(testData, buffer)) {
            throw new RuntimeException("Error");
          }
        }
      }
      System.out.println("Cache Hits    = " + hitsInCache.get());
      System.out.println("Cache Misses  = " + missesInCache.get());
      System.out.println("Store         = " + (storeTime / (double) passes)
          / 1000000.0);
      System.out.println("Fetch         = " + (fetchTime / (double) passes)
          / 1000000.0);
      System.out.println("# of Elements = " + blockCache.getSize());
      i++;
    }
  }

  private static byte[] testData(Random random, int size, byte[] buf) {
    random.nextBytes(buf);
    return buf;
  }
}
