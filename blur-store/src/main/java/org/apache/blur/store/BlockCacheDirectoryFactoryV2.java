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

import static org.apache.blur.utils.BlurConstants.SHARED_MERGE_SCHEDULER;

import java.io.IOException;
import java.util.Set;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.store.blockcache_v2.BaseCache;
import org.apache.blur.store.blockcache_v2.BaseCache.STORE;
import org.apache.blur.store.blockcache_v2.Cache;
import org.apache.blur.store.blockcache_v2.CacheDirectory;
import org.apache.blur.store.blockcache_v2.FileNameFilter;
import org.apache.blur.store.blockcache_v2.Quiet;
import org.apache.blur.store.blockcache_v2.Size;
import org.apache.lucene.store.Directory;

public class BlockCacheDirectoryFactoryV2 extends BlockCacheDirectoryFactory {

  private Cache _cache;

  public BlockCacheDirectoryFactoryV2(BlurConfiguration configuration, long totalNumberOfBytes) {
    final int fileBufferSizeInt = 8192;
    final int cacheBlockSizeInt = 8192;
    final STORE store = STORE.OFF_HEAP;

    Size fileBufferSize = new Size() {
      @Override
      public int getSize(String directoryName, String fileName) {
        return fileBufferSizeInt;
      }
    };

    Size cacheBlockSize = new Size() {
      @Override
      public int getSize(String directoryName, String fileName) {
        return cacheBlockSizeInt;
      }
    };

    FileNameFilter readFilter = new FileNameFilter() {
      @Override
      public boolean accept(String directoryName, String fileName) {
        if (fileName.endsWith(".fdt") || fileName.endsWith(".fdx")) {
          return false;
        }
        return true;
      }
    };

    FileNameFilter writeFilter = new FileNameFilter() {
      @Override
      public boolean accept(String directoryName, String fileName) {
        if (fileName.endsWith(".fdt") || fileName.endsWith(".fdx")) {
          return false;
        }
        return true;
      }
    };

    Quiet quiet = new Quiet() {
      @Override
      public boolean shouldBeQuiet(String directoryName, String fileName) {
        Thread thread = Thread.currentThread();
        String name = thread.getName();
        if (name.startsWith(SHARED_MERGE_SCHEDULER)) {
          return true;
        }
        return false;
      }
    };

    _cache = new BaseCache(totalNumberOfBytes, fileBufferSize, cacheBlockSize, readFilter, writeFilter, quiet, store);
  }

  @Override
  public Directory newDirectory(String table, String shard, Directory directory, Set<String> blockCacheFileTypes)
      throws IOException {
    return new CacheDirectory(table, shard, directory, _cache);
  }

}
