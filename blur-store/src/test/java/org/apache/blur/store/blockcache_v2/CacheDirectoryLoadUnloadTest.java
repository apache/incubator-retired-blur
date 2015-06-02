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

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.blur.store.blockcache.LastModified;
import org.apache.blur.store.blockcache_v2.BaseCache.STORE;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CacheDirectoryLoadUnloadTest {

  private BaseCache _cache;

//  @Before
  public void setup() throws IOException {
    long totalNumberOfBytes = 2000000000L;
    final int fileBufferSizeInt = 8192;
    final int cacheBlockSizeInt = 8192;
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
        STORE.OFF_HEAP);
  }

  @After
  public void tearDown() throws IOException {
    _cache.close();
  }

  private Directory newDirectory(File path) throws IOException {
    return new FSDir(path);
  }

  static class FSDir extends SimpleFSDirectory implements LastModified {

    public FSDir(File path) throws IOException {
      super(path);
    }

    @Override
    public long getFileModified(String name) throws IOException {
      return 0;
    }

  }

//  @Test
  public void test1() throws IOException {
    int i = 0;
    long length = 100000000;
    int numberOfFilesPerPass = 25;
    Random random = new Random(1);

    while (true) {
      CacheDirectory dir = getDir();
      for (int p = 0; p < numberOfFilesPerPass; p++) {
        String file = i + "-" + p + ".tim";
        System.out.println("Writing [" + file + "]");
        System.out.println("Cache Size " + _cache.getWeightedSize());

        IndexOutput output = dir.createOutput(file, IOContext.DEFAULT);
        write(output, length, random);
        output.close();
      }
      for (int p = 0; p < 10; p++) {
        String file = i + "-" + p + ".tim";
        System.out.println(dir.fileLength(file));
      }

      for (int p = 0; p < 10; p++) {
        String file = i + "-" + p + ".tim";
        System.out.println("Cache Size " + _cache.getWeightedSize());
        System.out.println("Reading [" + file + "]");
        IndexInput input = dir.openInput(file, IOContext.DEFAULT);
        read(input);
        input.close();
      }
      for (int p = 0; p < 10; p++) {
        String file = i + "-" + p + ".tim";
        dir.deleteFile(file);
      }
      i++;
      System.out.println("Cache Size " + _cache.getWeightedSize());
      dir.close();
      _cache.cleanupOldFiles();
    }
  }

  private CacheDirectory getDir() throws IOException {
    Directory directory = newDirectory(new File("./target/tmp/CacheDirectoryLoadUnloadTest"));
    BufferStore.initNewBuffer(1024, 1024 * 128);
    BufferStore.initNewBuffer(8192, 8192 * 128);
    return new CacheDirectory("test", "test", directory, _cache, null);
  }

  private void read(IndexInput input) throws IOException {
    byte[] buf = new byte[10000];
    long length = input.length();
    while (length > 0) {
      int len = (int) Math.min(length, buf.length);
      input.readBytes(buf, 0, len);
      length -= len;
    }
  }

  private void write(IndexOutput output, long length, Random random) throws IOException {
    byte[] buf = new byte[10000];
    while (length > 0) {
      random.nextBytes(buf);
      int len = (int) Math.min(length, buf.length);
      output.writeBytes(buf, len);
      length -= len;
    }
  }

}
