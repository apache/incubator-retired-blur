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
package org.apache.blur.store.hdfs;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.blur.HdfsMiniClusterUtil;
import org.apache.blur.memory.MemoryLeakDetector;
import org.apache.blur.utils.JavaHome;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HdfsDirectoryResourceTest {

  private static final long WRITE_SIZE = 10000000L;
  private static final long READ_SIZE = 2000000L;

  private static final int WAIT_TIME_IN_SECONDS = 10000;

  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir",
      "./target/tmp_HdfsDirectoryResourceTest"));

  private static Configuration _configuration = new Configuration();
  private static MiniDFSCluster _cluster;
  private static Path _root;

  private Random random = new Random();

  @BeforeClass
  public static void setupClass() throws IOException {
    JavaHome.checkJavaHome();
    _cluster = HdfsMiniClusterUtil.startDfs(_configuration, true, TMPDIR.getAbsolutePath());
    _root = new Path(_cluster.getFileSystem().getUri() + "/");
    MemoryLeakDetector.setEnabled(true);
  }

  @AfterClass
  public static void tearDown() {
    MemoryLeakDetector.setEnabled(false);
    HdfsMiniClusterUtil.shutdownDfs(_cluster);
  }

  @Test
  public void testResourceTracking() throws IOException, InterruptedException {
    Path path = new Path(_root, "testResourceTracking");
    boolean resourceTracking = true;
    HdfsDirectory dir = new HdfsDirectory(_configuration, path, null, null, resourceTracking);
    try {
      String name = "_1.file";
      executeWrites(dir, name);
      executeReads(dir, name);
      assertResourceCount(0);
      dir.deleteFile(name);
    } finally {
      dir.close();
    }
  }

  private void executeWrites(HdfsDirectory dir, String name) throws IOException {
    IndexOutput output = dir.createOutput(name, IOContext.DEFAULT);
    writeData(output, WRITE_SIZE);
    output.close();
  }

  private void executeReads(HdfsDirectory dir, String name) throws IOException, InterruptedException {
    IndexInput input = dir.openInput(name, IOContext.READ);
    assertResourceCount(1);
    input.readLong();
    input.seek(0L);
    for (int i = 0; i < 2; i++) {
      readSeq(input.clone(), READ_SIZE);
      assertResourceCount(1 + i + 1);
    }
    input.close();
  }

  private void assertResourceCount(int count) throws InterruptedException {
    for (int i = 0; i < WAIT_TIME_IN_SECONDS; i++) {
      Thread.sleep(1000);
      Runtime.getRuntime().gc();
      Runtime.getRuntime().gc();
      int memLeakDet = MemoryLeakDetector.getCount();
      if (memLeakDet == count) {
        return;
      } else {
        System.out.println("MemoryLeakDetector [" + memLeakDet + "] assertion [" + count + "]");
      }
    }
    fail();
  }

  private void readSeq(IndexInput input, long read) throws IOException {
    byte[] buf = new byte[1024];
    while (read > 0) {
      int len = (int) Math.min(buf.length, read);
      input.readBytes(buf, 0, len);
      read -= len;
    }
  }

  private void writeData(IndexOutput output, long write) throws IOException {
    byte[] buf = new byte[1024];
    while (write > 0) {
      random.nextBytes(buf);
      int length = (int) Math.min(write, buf.length);
      output.writeBytes(buf, length);
      write -= length;
    }
  }
}
