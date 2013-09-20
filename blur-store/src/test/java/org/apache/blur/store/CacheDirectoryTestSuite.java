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
import org.apache.blur.store.blockcache_v2.BaseCache.STORE;
import org.apache.blur.store.blockcache_v2.Cache;
import org.apache.blur.store.blockcache_v2.CacheDirectory;
import org.apache.blur.store.blockcache_v2.FileNameBlockSize;
import org.apache.blur.store.blockcache_v2.FileNameFilter;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

public class CacheDirectoryTestSuite extends BaseDirectoryTestSuite {

  @Override
  protected void setupDirectory() throws IOException {
    int totalNumberOfBytes = 1000000;
    int fileBufferSize = 129;
    final int blockSize = 137;
    FileNameBlockSize fileNameBlockSize = new FileNameBlockSize() {
      @Override
      public int getBlockSize(String directoryName, String fileName) {
        return blockSize;
      }
    };
    FileNameFilter writeFilter = new FileNameFilter() {
      @Override
      public boolean accept(String directoryName, String fileName) {
        return true;
      }
    };
    FileNameFilter readFilter = new FileNameFilter() {
      @Override
      public boolean accept(String directoryName, String fileName) {
        // @TODO this needs to be enabled and issues resolved...
        return false;
      }
    };

    Cache cache = new BaseCache(totalNumberOfBytes, fileBufferSize, fileNameBlockSize, readFilter, writeFilter,
        STORE.ON_HEAP);
    Directory dir = FSDirectory.open(new File(file, "cache"));

    BufferStore.init(128, 128);
    directory = new CacheDirectory("test", wrapLastModified(dir), cache);
  }

  @Test
  public void runsTheTests() {
  }

}
