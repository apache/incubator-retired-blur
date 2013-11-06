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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.blur.store.buffer.BufferStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Before;
import org.junit.Test;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

public class BlockDirectoryTest {
  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "/tmp"));

  private class MapperCache implements Cache {
    public Map<String, byte[]> map = new ConcurrentLinkedHashMap.Builder<String, byte[]>().maximumWeightedCapacity(8).build();

    @Override
    public void update(String name, long blockId, int blockOffset, byte[] buffer, int offset, int length) {
      byte[] cached = map.get(name + blockId);
      if (cached != null) {
        int newlen = Math.max(cached.length, blockOffset + length);
        byte[] b = new byte[newlen];
        System.arraycopy(cached, 0, b, 0, cached.length);
        System.arraycopy(buffer, offset, b, blockOffset, length);
        cached = b;
      } else {
        cached = copy(blockOffset, buffer, offset, length);
      }
      map.put(name + blockId, cached);
    }

    private byte[] copy(int blockOffset, byte[] buffer, int offset, int length) {
      byte[] b = new byte[length + blockOffset];
      System.arraycopy(buffer, offset, b, blockOffset, length);
      return b;
    }

    @Override
    public boolean fetch(String name, long blockId, int blockOffset, byte[] b, int off, int lengthToReadInBlock) {
      // return false;
      byte[] data = map.get(name + blockId);
      if (data == null) {
        return false;
      }
      System.arraycopy(data, blockOffset, b, off, lengthToReadInBlock);
      return true;
    }

    @Override
    public void delete(String name) {

    }

    @Override
    public long size() {
      return map.size();
    }

    @Override
    public void renameCacheFile(String source, String dest) {
    }
  }

  private static final int MAX_NUMBER_OF_WRITES = 10000;
  private static final int MIN_FILE_SIZE = 100;
  private static final int MAX_FILE_SIZE = 100000;
  private static final int MIN_BUFFER_SIZE = 1;
  private static final int MAX_BUFFER_SIZE = 12000;
  private static final int MAX_NUMBER_OF_READS = 20000;
  private Directory directory;
  private File file;
  private long seed;
  private Random random;
  private MapperCache mapperCache;
  
  @Before
  public void setUp() throws IOException {
    BufferStore.initNewBuffer(1024, 1024 * 128);
    BufferStore.initNewBuffer(8192, 8192 * 128);
    file = new File(TMPDIR, "blockdirectorytest");
    rm(file);
    file.mkdirs();
    FSDirectory dir = FSDirectory.open(new File(file, "base"));
    mapperCache = new MapperCache();
    directory = new BlockDirectory("test", dir, mapperCache);
    seed = new Random().nextLong();
    System.out.println("Seed is " + seed);
    random = new Random(seed);
  }

  @Test
  public void testEOF() throws IOException {
    Directory fsDir = FSDirectory.open(new File(file, "normal"));
    String name = "test.eof";
    createFile(name, fsDir, directory);
    long fsLength = fsDir.fileLength(name);
    long hdfsLength = directory.fileLength(name);
    assertEquals(fsLength, hdfsLength);
    testEof(name, fsDir, fsLength);
    testEof(name, directory, hdfsLength);
  }

  private void testEof(String name, Directory directory, long length) throws IOException {
    IndexInput input = directory.openInput(name, IOContext.DEFAULT);
    input.seek(length);
    try {
      input.readByte();
      fail("should throw eof");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRandomAccessWrites() throws IOException {
    long t1 = System.nanoTime();

    int i = 0;
    try {
      for (; i < 10; i++) {
        Directory fsDir = FSDirectory.open(new File(file, "normal"));
        String name = getName();
        createFile(name, fsDir, directory);
        assertInputsEquals(name, fsDir, directory);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed with seed [" + seed + "] on pass [" + i + "]");
    }
    long t2 = System.nanoTime();
    System.out.println("Total time is " + ((t2 - t1)/1000000) + "ms");
  }

  @Test
  public void testRandomAccessWritesLargeCache() throws IOException {
    mapperCache.map = new ConcurrentLinkedHashMap.Builder<String, byte[]>().maximumWeightedCapacity(10000).build();
    testRandomAccessWrites();
  }

  private void assertInputsEquals(String name, Directory fsDir, Directory hdfs) throws IOException {
    int reads = random.nextInt(MAX_NUMBER_OF_READS);
    IndexInput fsInput = fsDir.openInput(name, IOContext.DEFAULT);
    IndexInput hdfsInput = hdfs.openInput(name, IOContext.DEFAULT);
    assertEquals(fsInput.length(), hdfsInput.length());
    int fileLength = (int) fsInput.length();
    if (fileLength != 0) {
      for (int i = 0; i < reads; i++) {
        byte[] fsBuf = new byte[random.nextInt(Math.min(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE, fileLength)) + MIN_BUFFER_SIZE];
        byte[] hdfsBuf = new byte[fsBuf.length];
        int offset = random.nextInt(fsBuf.length);
        int length = random.nextInt(fsBuf.length - offset);
        int pos = random.nextInt(fileLength - length);
        fsInput.seek(pos);
        fsInput.readBytes(fsBuf, offset, length);
        hdfsInput.seek(pos);
        hdfsInput.readBytes(hdfsBuf, offset, length);
        for (int f = offset; f < length; f++) {
          if (fsBuf[f] != hdfsBuf[f]) {
            fail(Long.toString(seed) + " read [" + i + "]");
          }
        }
      } 
    }
    fsInput.close();
    hdfsInput.close();
  }

  private void createFile(String name, Directory fsDir, Directory hdfs) throws IOException {
    int writes = random.nextInt(MAX_NUMBER_OF_WRITES);
    int fileLength = random.nextInt(MAX_FILE_SIZE - MIN_FILE_SIZE) + MIN_FILE_SIZE;
    IndexOutput fsOutput = fsDir.createOutput(name, IOContext.DEFAULT);
    IndexOutput hdfsOutput = hdfs.createOutput(name, IOContext.DEFAULT);
    for (int i = 0; i < writes; i++) {
      byte[] buf = new byte[random.nextInt(Math.min(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE, fileLength)) + MIN_BUFFER_SIZE];
      random.nextBytes(buf);
      int offset = random.nextInt(buf.length);
      int length = random.nextInt(buf.length - offset);
      fsOutput.writeBytes(buf, offset, length);
      hdfsOutput.writeBytes(buf, offset, length);
    }
    fsOutput.close();
    hdfsOutput.close();
  }

  private String getName() {
    return Long.toString(Math.abs(random.nextLong()));
  }

  public static void rm(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }

}
