package com.nearinfinity.blur.store.blockcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.commons.collections.map.LRUMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.store.DirectIODirectory;

public class BlockDirectoryTest {

  private static final int MAX_NUMBER_OF_WRITES = 10000;
  private static final int MIN_FILE_SIZE = 100;
  private static final int MAX_FILE_SIZE = 100000;
  private static final int MIN_BUFFER_SIZE = 1;
  private static final int MAX_BUFFER_SIZE = 5000;
  private static final int MAX_NUMBER_OF_READS = 20000;
  private Directory directory;
  private File file;
  private long seed;
  private Random random;

  @Before
  public void setUp() throws IOException {
    file = new File("./tmp");
    rm(file);
    file.mkdirs();
    FSDirectory dir = FSDirectory.open(new File(file, "base"));
    directory = new BlockDirectory("test", DirectIODirectory.wrap(dir), getBasicCache());
    seed = new Random().nextLong();
    seed = -7671132935535364673L;

    random = new Random(seed);
  }

  @SuppressWarnings("unchecked")
  private Cache getBasicCache() {
    return new Cache() {
      private Map<String, byte[]> map = Collections.synchronizedMap(new LRUMap(8));

      @Override
      public void update(String name, long blockId, byte[] buffer) {
        map.put(name + blockId, copy(buffer));
      }

      private byte[] copy(byte[] buffer) {
        byte[] b = new byte[buffer.length];
        System.arraycopy(buffer, 0, b, 0, buffer.length);
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
    };
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
    IndexInput input = directory.openInput(name);
    input.seek(length);
    try {
      input.readByte();
      fail("should throw eof");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRandomAccessWrites() throws IOException {
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
  }

  private void assertInputsEquals(String name, Directory fsDir, Directory hdfs) throws IOException {
    int reads = random.nextInt(MAX_NUMBER_OF_READS);
    int buffer = random.nextInt(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE) + MIN_BUFFER_SIZE;
    IndexInput fsInput = fsDir.openInput(name, buffer);
    IndexInput hdfsInput = hdfs.openInput(name, buffer);
    assertEquals(fsInput.length(), hdfsInput.length());
    int fileLength = (int) fsInput.length();
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
    fsInput.close();
    hdfsInput.close();
  }

  private void createFile(String name, Directory fsDir, Directory hdfs) throws IOException {
    int writes = random.nextInt(MAX_NUMBER_OF_WRITES);
    int fileLength = random.nextInt(MAX_FILE_SIZE - MIN_FILE_SIZE) + MIN_FILE_SIZE;
    IndexOutput fsOutput = fsDir.createOutput(name);
    fsOutput.setLength(fileLength);
    IndexOutput hdfsOutput = hdfs.createOutput(name);
    hdfsOutput.setLength(fileLength);
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
