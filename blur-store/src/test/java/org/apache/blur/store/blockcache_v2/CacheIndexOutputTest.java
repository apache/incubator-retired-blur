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

import org.apache.blur.store.buffer.BufferStore;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;

public class CacheIndexOutputTest {

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
    Random random = new Random(seed);
    RAMDirectory directory = new RAMDirectory();

    Cache cache = CacheIndexInputTest.getCache();
    CacheIndexOutput indexOutput = new CacheIndexOutput(null, "test", cache, directory, IOContext.DEFAULT);
    indexOutput.writeByte((byte) 1);
    indexOutput.writeByte((byte) 2);
    byte[] b = new byte[16000];
    random.nextBytes(b);
    indexOutput.writeBytes(b, 16000);
    indexOutput.close();

    IndexInput input = directory.openInput("test", IOContext.DEFAULT);
    assertEquals(16002, input.length());
    assertEquals(1, input.readByte());
    assertEquals(2, input.readByte());

    byte[] buf = new byte[16000];
    input.readBytes(buf, 0, 16000);
    input.close();
    assertArrayEquals(b, buf);
    directory.close();
  }

  @Test
  public void test2() throws IOException {
    Cache cache = CacheIndexInputTest.getCache();
    RAMDirectory directory = new RAMDirectory();
    RAMDirectory directory2 = new RAMDirectory();

    Random random = new Random(seed);

    String name = "test2";
    long size = (10 * 1024 * 1024) + 13;

    IndexOutput output = directory.createOutput(name, IOContext.DEFAULT);
    CacheIndexOutput cacheIndexOutput = new CacheIndexOutput(null, name, cache, directory2, IOContext.DEFAULT);
    CacheIndexInputTest.writeRandomData(size, random, output, cacheIndexOutput);
    output.close();
    cacheIndexOutput.close();

    IndexInput input = directory.openInput(name, IOContext.DEFAULT);
    IndexInput testInput = directory2.openInput(name, IOContext.DEFAULT);
    CacheIndexInputTest.readRandomData(input, testInput, random, sampleSize, maxBufSize, maxOffset);
    testInput.close();
    input.close();
    directory.close();
    directory2.close();
  }

}
