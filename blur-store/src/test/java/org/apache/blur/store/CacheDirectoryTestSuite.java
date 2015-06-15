package org.apache.blur.store;

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
import java.io.File;
import java.io.IOException;

import org.apache.blur.store.blockcache_v2.BaseCache;
import org.apache.blur.store.blockcache_v2.BaseCacheValueBufferPool;
import org.apache.blur.store.blockcache_v2.CacheDirectory;
import org.apache.blur.store.blockcache_v2.FileNameFilter;
import org.apache.blur.store.blockcache_v2.Quiet;
import org.apache.blur.store.blockcache_v2.Size;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public abstract class CacheDirectoryTestSuite extends BaseDirectoryTestSuite {

  private BaseCache _cache;

  @Override
  protected Directory setupDirectory() throws IOException {
    int totalNumberOfBytes = 1000000;
    final int fileBufferSizeInt = numberBetween(113, 215);
    final int cacheBlockSizeInt = numberBetween(111, 251);

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
    _cache = new BaseCache(totalNumberOfBytes, fileBufferSize, cacheBlockSize, readFilter, writeFilter, quiet,
        getPool());
    Directory dir = FSDirectory.open(new File(file, "cache"));

    BufferStore.initNewBuffer(1024, 1024 * 128);
    BufferStore.initNewBuffer(8192, 8192 * 128);
    return new CacheDirectory("test", "test", wrapLastModified(dir), _cache, null);
  }

  protected abstract BaseCacheValueBufferPool getPool();

  public void close() throws IOException {
    _cache.close();
  }

}
