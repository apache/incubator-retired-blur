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
package org.apache.blur.store.blockcache_v2;

import java.io.IOException;

import org.apache.blur.store.blockcache.LastModified;
import org.apache.blur.store.blockcache_v2.BaseCache.STORE;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

public class BaseCacheTest {

  private BaseCache _cache;
  private CacheDirectory _cacheDirectory;

  @Test
  public void test1() {
    final int totalNumberOfBytes = 1000000;
    final int fileBufferSizeInt = 127;
    final int cacheBlockSizeInt = 131;
    Size fileBufferSize = new Size() {
      @Override
      public int getSize(CacheDirectory directory, String fileName) {
        return fileBufferSizeInt;
      }
    };
    Size cacheBlockSize = new Size() {
      @Override
      public int getSize(CacheDirectory directory, String fileName) {
        return cacheBlockSizeInt;
      }
    };
    Size directLocalCacheLimit = new Size() {
      @Override
      public int getSize(CacheDirectory directory, String fileName) {
        return totalNumberOfBytes;
      }
    };
    FileNameFilter writeFilter = new FileNameFilter() {
      @Override
      public boolean accept(CacheDirectory directory, String fileName) {
        return true;
      }
    };
    FileNameFilter readFilter = new FileNameFilter() {
      @Override
      public boolean accept(CacheDirectory directory, String fileName) {
        return true;
      }
    };
    Quiet quiet = new Quiet() {
      @Override
      public boolean shouldBeQuiet(CacheDirectory directory, String fileName) {
        return false;
      }
    };
    SimpleCacheValueBufferPool simpleCacheValueBufferPool = new SimpleCacheValueBufferPool(STORE.ON_HEAP, 1000);
    _cache = new BaseCache(totalNumberOfBytes, fileBufferSize, cacheBlockSize, directLocalCacheLimit, readFilter, writeFilter, quiet,
        simpleCacheValueBufferPool);

    Directory directory = newDirectory();
    BufferStore.initNewBuffer(1024, 1024 * 128);
    BufferStore.initNewBuffer(8192, 8192 * 128);
    _cacheDirectory = new CacheDirectory("test", "test", directory, _cache, null);

    String fileName = "test";
    {
      CacheKey key = new CacheKey();
      CacheValue value = _cache.newInstance(_cacheDirectory, fileName);
      _cache.put(_cacheDirectory, fileName, key, value);
    }
    {
      CacheKey key = new CacheKey();
      CacheValue value = _cache.newInstance(_cacheDirectory, fileName);
      _cache.put(_cacheDirectory, fileName, key, value);
    }
  }

  private Directory newDirectory() {
    return new RDir();
  }

  static class RDir extends RAMDirectory implements LastModified {
    @Override
    public long getFileModified(String name) throws IOException {
      return 0;
    }
  }
}
