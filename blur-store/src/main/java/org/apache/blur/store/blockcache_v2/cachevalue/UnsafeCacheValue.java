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

import static org.apache.blur.metrics.MetricsConstants.CACHE_VALUE_FINALIZE;
import static org.apache.blur.metrics.MetricsConstants.JVM;
import static org.apache.blur.metrics.MetricsConstants.OFF_HEAP_MEMORY;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.metrics.AtomicLongGauge;
import org.apache.blur.store.util.UnsafeUtil;

import sun.misc.Unsafe;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;

public class UnsafeCacheValue extends BaseCacheValue {

  private static final AtomicLong _neededFinalizedCall = new AtomicLong();

  static {
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, JVM, CACHE_VALUE_FINALIZE), new AtomicLongGauge(
        _neededFinalizedCall));
  }

  private static final Unsafe _unsafe;
  private static final AtomicLong _offHeapMemorySize = new AtomicLong();

  static {
    _unsafe = UnsafeUtil.getUnsafe();
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, JVM, OFF_HEAP_MEMORY), new AtomicLongGauge(_offHeapMemorySize));
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

  public UnsafeCacheValue(int length) {
    super(length);
    _address = _unsafe.allocateMemory(_length);
    _offHeapMemorySize.addAndGet(_length);
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
      _offHeapMemorySize.addAndGet(0 - _length);
    }
  }

  // This is commented out normally. Add code when debugging memory related
  // issues.
  // @Override
  // protected void finalize() throws Throwable {
  // if (!_released) {
  // new Throwable().printStackTrace();
  // System.exit(1);
  // release();
  // _neededFinalizedCall.incrementAndGet();
  // }
  // }
}
