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

import java.lang.reflect.Field;

import sun.misc.Unsafe;

@SuppressWarnings("serial")
public class UnsafeCacheValue extends BaseCacheValue {

  private static final int MINIMUM_SIZE = 1024;

  private static final Unsafe _unsafe;

  static {
    try {
      Class<?> clazz = Class.forName("java.nio.Bits");
      Field field = clazz.getDeclaredField("unsafe");
      field.setAccessible(true);
      _unsafe = (Unsafe) field.get(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final int BYTE_ARRAY_BASE_OFFSET = _unsafe.arrayBaseOffset(byte[].class);

  private static void copyFromArray(byte[] src, int srcOffset, int length, long destAddress) {
    long offset = BYTE_ARRAY_BASE_OFFSET + srcOffset;
    _unsafe.copyMemory(src, offset, null, destAddress, length);
  }

  private static void copyToArray(long srcAddress, byte[] dst, int dstOffset, int length) {
    long offset = BYTE_ARRAY_BASE_OFFSET + dstOffset;
    _unsafe.copyMemory(null, srcAddress, dst, offset, length);
  }

  private final long _address;
  private final int _capacity;
  private volatile boolean _released = false;

  public UnsafeCacheValue(int length) {
    super(length);
    _capacity = getCapacity(length);
    _address = _unsafe.allocateMemory(_capacity);
  }

  private int getCapacity(int length) {
//    if (length < MINIMUM_SIZE) {
//      return MINIMUM_SIZE;
//    }
    return length;
  }

  @Override
  protected void writeInternal(int position, byte[] buf, int offset, int length) {
    copyFromArray(buf, offset, length, resolveAddress(position));
  }

  @Override
  protected void readInternal(int position, byte[] buf, int offset, int length) {
    copyToArray(resolveAddress(position), buf, offset, length);
  }

  @Override
  protected byte readInternal(int position) {
    return _unsafe.getByte(resolveAddress(position));
  }

  private long resolveAddress(int position) {
    return _address + position;
  }

  @Override
  public void release() {
    if (!_released) {
      _unsafe.freeMemory(_address);
      _released = true;
    } else {
      System.out.println("released twice?");
      new Throwable().printStackTrace();
    }
  }

  @Override
  public int size() {
    return _capacity;
  }

}
