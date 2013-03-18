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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;

public class HdfsDirectoryTest {
  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "/tmp"));

  private static final int MAX_NUMBER_OF_WRITES = 10000;
  private static final int MIN_FILE_SIZE = 100;
  private static final int MAX_FILE_SIZE = 100000;
  private static final int MIN_BUFFER_SIZE = 1;
  private static final int MAX_BUFFER_SIZE = 5000;
  private static final int MAX_NUMBER_OF_READS = 10000;
  private HdfsDirectory directory;
  private File file;
  private long seed;
  private Random random;

  @Before
  public void setUp() throws IOException {
    BufferStore.init(128, 128);
    file = new File(TMPDIR, "hdfsdirectorytest");
    rm(file);
    URI uri = new File(file, "hdfs").toURI();
    Path hdfsDirPath = new Path(uri.toString());
    Configuration conf = new Configuration();
    directory = new HdfsDirectory(conf, hdfsDirPath);
    seed = new Random().nextLong();
    random = new Random(seed);
  }

  @Test
  public void testWritingAndReadingAFile() throws IOException {

    IndexOutput output = directory.createOutput("testing.test", IOContext.DEFAULT);
    output.writeInt(12345);
    output.flush();
    output.close();

    IndexInput input = directory.openInput("testing.test", IOContext.DEFAULT);
    assertEquals(12345, input.readInt());
    input.close();

    String[] listAll = directory.listAll();
    assertEquals(1, listAll.length);
    assertEquals("testing.test", listAll[0]);

    assertEquals(4, directory.fileLength("testing.test"));

    IndexInput input1 = directory.openInput("testing.test", IOContext.DEFAULT);

    IndexInput input2 = (IndexInput) input1.clone();
    assertEquals(12345, input2.readInt());
    input2.close();

    assertEquals(12345, input1.readInt());
    input1.close();

    assertFalse(directory.fileExists("testing.test.other"));
    assertTrue(directory.fileExists("testing.test"));
    directory.deleteFile("testing.test");
    assertFalse(directory.fileExists("testing.test"));
  }

  @Test
  public void testEOF() throws IOException {
    Directory fsDir = new RAMDirectory();
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
  public void testWrites() throws IOException {
    int i = 0;
    try {
      Set<String> names = new HashSet<String>();
      for (; i < 10; i++) {
        Directory fsDir = new RAMDirectory();
        String name = getName();
        System.out.println("Working on pass [" + i + "] seed [" + seed + "] contains [" + names.contains(name) + "]");
        names.add(name);
        createFile(name, fsDir, directory);
        assertInputsEquals(name, fsDir, directory);
        fsDir.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed with seed [" + seed + "] on pass [" + i + "]");
    }
  }

  private void assertInputsEquals(String name, Directory fsDir, HdfsDirectory hdfs) throws IOException {
    int reads = random.nextInt(MAX_NUMBER_OF_READS);
    IndexInput fsInput = fsDir.openInput(name, IOContext.DEFAULT);
    IndexInput hdfsInput = hdfs.openInput(name, IOContext.DEFAULT);
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
          fail();
        }
      }
    }
    fsInput.close();
    hdfsInput.close();
  }

  private void createFile(String name, Directory fsDir, HdfsDirectory hdfs) throws IOException {
    int writes = random.nextInt(MAX_NUMBER_OF_WRITES);
    int fileLength = random.nextInt(MAX_FILE_SIZE - MIN_FILE_SIZE) + MIN_FILE_SIZE;
    IndexOutput fsOutput = fsDir.createOutput(name, IOContext.DEFAULT);
    fsOutput.setLength(fileLength);
    IndexOutput hdfsOutput = hdfs.createOutput(name, IOContext.DEFAULT);
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
