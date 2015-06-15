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
package org.apache.blur.store.blockcache_v2.cachevalue;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Random;

import org.apache.blur.store.blockcache_v2.EvictionException;
import org.junit.Test;

import sun.misc.Unsafe;

public class UnsafeWrapperMultiCacheValueTest {

  @Test
  public void testUnsafeWrapperMultiCacheValue() throws EvictionException {

    final Unsafe unsafe = UnsafeCacheValue._unsafe;
    final long[] addresses = new long[10];
    int chunkSize = 1000;
    int chunks = 10;
    for (int i = 0; i < chunks; i++) {
      addresses[i] = unsafe.allocateMemory(chunkSize);
    }

    int length = addresses.length * chunkSize;

    UnsafeWrapperMultiCacheValue unsafeWrapperMultiCacheValue = new UnsafeWrapperMultiCacheValue(length, addresses,
        chunkSize) {
      @Override
      protected void releaseInternal() {
        for (int i = 0; i < 10; i++) {
          unsafe.freeMemory(addresses[i]);
        }
      }
    };

    Random random = new Random();
    byte[] buf1 = new byte[length];
    random.nextBytes(buf1);

    unsafeWrapperMultiCacheValue.write(0, buf1, 0, length);

    byte[] buf2 = new byte[length];

    unsafeWrapperMultiCacheValue.read(0, buf2, 0, length);

    assertTrue(equals(buf1, buf2));
    assertTrue(Arrays.equals(buf1, buf2));

    unsafeWrapperMultiCacheValue.release();

  }

  public static boolean equals(byte[] a, byte[] a2) {
    if (a == a2)
      return true;
    if (a == null || a2 == null)
      return false;

    int length = a.length;
    if (a2.length != length)
      return false;

    for (int i = 0; i < length; i++)
      if (a[i] != a2[i]) {
        System.out.println("Pos [" + i + "]");
        return false;
      }

    return true;
  }

}
