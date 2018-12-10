/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.store.blockcache_v2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;

import org.apache.blur.store.blockcache_v2.cachevalue.ByteArrayCacheValue;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;

public class CacheIndexInputTest {

  private long seed;

  private final int sampleSize = 10000;
  private final int maxBufSize = 10000;
  private final int maxOffset = 1000;

  @Before
  public void setup() {
    BufferStore.initNewBuffer(1024, 1024 * 128);
    BufferStore.initNewBuffer(8192, 8192 * 128);
    seed = new Random().nextLong();
    System.out.println("Using seed [" + seed + "]");
    // seed = -265282183286396219l;
  }

  @Test
  public void test1() throws IOException {
    RAMDirectory directory = new RAMDirectory();

    String name = "test";

    IndexOutput output = directory.createOutput(name, IOContext.DEFAULT);
    byte[] bs = "hello world".getBytes();
    output.writeBytes(bs, bs.length);
    output.close();

    IndexInput input = directory.openInput(name, IOContext.DEFAULT);
    Cache cache = getCache();
    CacheIndexInput cacheInput = new CacheIndexInput(null, name, input, cache);
    byte[] buf = new byte[bs.length];
    cacheInput.readBytes(buf, 0, buf.length);
    cacheInput.close();

    assertArrayEquals(bs, buf);
    directory.close();
  }

  @Test
  public void test2() throws IOException {
    Cache cache = getCache();
    RAMDirectory directory = new RAMDirectory();
    Random random = new Random(seed);

    String name = "test2";
    long size = (10 * 1024 * 1024) + 13;

    IndexOutput output = directory.createOutput(name, IOContext.DEFAULT);
    writeRandomData(size, random, output);
    output.close();

    IndexInput input = directory.openInput(name, IOContext.DEFAULT);
    IndexInput testInput = new CacheIndexInput(null, name, input.clone(), cache);
    readRandomData(input, testInput, random, sampleSize, maxBufSize, maxOffset);
    readRandomDataShort(input, testInput, random, sampleSize);
    readRandomDataInt(input, testInput, random, sampleSize);
    readRandomDataLong(input, testInput, random, sampleSize);
    testInput.close();
    input.close();
    directory.close();
  }

  public static void readRandomData(IndexInput baseInput, IndexInput testInput, Random random, int sampleSize,
      int maxBufSize, int maxOffset) throws IOException {
    assertEquals(baseInput.length(), testInput.length());
    int fileLength = (int) baseInput.length();
    for (int i = 0; i < sampleSize; i++) {
      int position = random.nextInt(fileLength - maxBufSize);
      int bufSize = random.nextInt(maxBufSize - maxOffset) + 1;
      byte[] buf1 = new byte[bufSize];
      byte[] buf2 = new byte[bufSize];

      int offset = random.nextInt(Math.min(maxOffset, bufSize));
      int len = Math.min(random.nextInt(bufSize - offset), fileLength - position);

      baseInput.seek(position);
      baseInput.readBytes(buf1, offset, len);
      testInput.seek(position);
      testInput.readBytes(buf2, offset, len);
      assertArrayEquals("Read [" + i + "] The position is [" + position + "] and bufSize [" + bufSize + "]", buf1, buf2);
    }
  }

  public static void readRandomDataInt(IndexInput baseInput, IndexInput testInput, Random random, int sampleSize)
      throws IOException {
    assertEquals(baseInput.length(), testInput.length());
    int fileLength = (int) baseInput.length();
    for (int i = 0; i < sampleSize; i++) {
      int position = random.nextInt(fileLength - 4);
      baseInput.seek(position);
      int i1 = baseInput.readInt();
      testInput.seek(position);
      int i2 = testInput.readInt();
      assertEquals("Read [" + i + "] The position is [" + position + "]", i1, i2);
    }
  }

  public static void readRandomDataShort(IndexInput baseInput, IndexInput testInput, Random random, int sampleSize)
      throws IOException {
    assertEquals(baseInput.length(), testInput.length());
    int fileLength = (int) baseInput.length();
    for (int i = 0; i < sampleSize; i++) {
      int position = random.nextInt(fileLength - 2);
      baseInput.seek(position);
      short i1 = baseInput.readShort();
      testInput.seek(position);
      short i2 = testInput.readShort();
      assertEquals("Read [" + i + "] The position is [" + position + "]", i1, i2);
    }
  }

  public static void readRandomDataLong(IndexInput baseInput, IndexInput testInput, Random random, int sampleSize)
      throws IOException {
    assertEquals(baseInput.length(), testInput.length());
    int fileLength = (int) baseInput.length();
    for (int i = 0; i < sampleSize; i++) {
      int position = random.nextInt(fileLength - 8);
      baseInput.seek(position);
      long i1 = baseInput.readLong();
      testInput.seek(position);
      long i2 = testInput.readLong();
      assertEquals("Read [" + i + "] The position is [" + position + "]", i1, i2);
    }
  }

  public static void writeRandomData(long size, Random random, IndexOutput... outputs) throws IOException {
    byte[] buf = new byte[1024];
    for (long l = 0; l < size; l += buf.length) {
      random.nextBytes(buf);
      int length = (int) Math.min(buf.length, size - l);
      for (IndexOutput output : outputs) {
        output.writeBytes(buf, length);
      }
    }
  }

  public static Cache getCache() {
    EvictionListener<CacheKey, CacheValue> listener = new EvictionListener<CacheKey, CacheValue>() {
      @Override
      public void onEviction(CacheKey key, CacheValue value) {
        value.release();
      }
    };
    Weigher<CacheValue> weigher = new Weigher<CacheValue>() {
      @Override
      public int weightOf(CacheValue value) {
        try {
          return value.length();
        } catch (EvictionException e) {
          return 0;
        }
      }
    };
    long maximumWeightedCapacity = 1 * 1024 * 1024;
    final ConcurrentLinkedHashMap<CacheKey, CacheValue> cache = new ConcurrentLinkedHashMap.Builder<CacheKey, CacheValue>()
        .weigher(weigher).maximumWeightedCapacity(maximumWeightedCapacity).listener(listener).build();
    Cache cacheFactory = new Cache() {

      @Override
      public CacheValue newInstance(CacheDirectory directory, String fileName, int cacheBlockSize) {
        return new ByteArrayCacheValue(cacheBlockSize);
      }

      @Override
      public long getFileId(CacheDirectory directory, String fileName) {
        return fileName.hashCode();
      }

      @Override
      public int getFileBufferSize(CacheDirectory directory, String fileName) {
        return 1024;
      }

      @Override
      public int getCacheBlockSize(CacheDirectory directory, String fileName) {
        return 8192;
      }

      @Override
      public boolean cacheFileForReading(CacheDirectory directory, String name, IOContext context) {
        return true;
      }

      @Override
      public boolean cacheFileForWriting(CacheDirectory directory, String name, IOContext context) {
        return true;
      }

      @Override
      public CacheValue get(CacheDirectory directory, String fileName, CacheKey key) {
        return cache.get(key);
      }

      @Override
      public void put(CacheDirectory directory, String fileName, CacheKey key, CacheValue value) {
        cache.put(key, value);
      }

      @Override
      public void removeFile(CacheDirectory directory, String fileName) throws IOException {

      }

      @Override
      public void releaseDirectory(CacheDirectory directory) {

      }

      @Override
      public CacheValue getQuietly(CacheDirectory directory, String fileName, CacheKey key) {
        return cache.getQuietly(key);
      }

      @Override
      public boolean shouldBeQuiet(CacheDirectory directory, String fileName) {
        return false;
      }

      @Override
      public void close() throws IOException {

      }

      @Override
      public void fileClosedForWriting(CacheDirectory directory, String fileName, long fileId) throws IOException {

      }

    };
    return cacheFactory;
  }
}
