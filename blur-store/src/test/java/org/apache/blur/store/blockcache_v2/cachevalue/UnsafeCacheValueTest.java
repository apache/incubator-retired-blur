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
package org.apache.blur.store.blockcache_v2.cachevalue;

import static org.junit.Assert.*;

import org.apache.blur.store.blockcache_v2.EvictionException;
import org.junit.Test;

public class UnsafeCacheValueTest {

  @Test
  public void test1() throws EvictionException {
    UnsafeCacheValue value = new UnsafeCacheValue(10);
    byte[] buf = "hello world".getBytes();
    value.write(0, buf, 0, 10);
    byte[] buf2 = new byte[10];
    value.read(0, buf2, 0, 10);
    assertArrayEquals("hello worl".getBytes(), buf2);
    value.release();
  }

  @Test
  public void test2() {
    UnsafeCacheValue value = new UnsafeCacheValue(10);
    byte[] buf = "hello world".getBytes();
    try {
      value.write(0, buf, 0, 11);
      fail();
    } catch (ArrayIndexOutOfBoundsException e) {
    }
    value.release();
  }

  @Test
  public void test3() {
    UnsafeCacheValue value = new UnsafeCacheValue(10);
    byte[] buf = "hello world".getBytes();
    try {
      value.write(8, buf, 0, 3);
      fail();
    } catch (ArrayIndexOutOfBoundsException e) {
    }
    value.release();
  }

  @Test
  public void test4() throws EvictionException {
    UnsafeCacheValue value = new UnsafeCacheValue(10);
    byte[] buf = "hello world".getBytes();
    value.write(8, buf, 0, 2);

    assertEquals('h', value.read(8));
    assertEquals('e', value.read(9));
    value.release();
  }

}
