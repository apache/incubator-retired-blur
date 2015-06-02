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

public abstract class UnsafeCacheValue extends BaseCacheValue {

  protected static final AtomicLong _neededFinalizedCall = new AtomicLong();

  static {
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, JVM, CACHE_VALUE_FINALIZE), new AtomicLongGauge(
        _neededFinalizedCall));
  }

  protected static final Unsafe _unsafe;
  protected static final AtomicLong _offHeapMemorySize = new AtomicLong();

  static {
    _unsafe = UnsafeUtil.getUnsafe();
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, JVM, OFF_HEAP_MEMORY), new AtomicLongGauge(_offHeapMemorySize));
  }

  private static final int BYTE_ARRAY_BASE_OFFSET = _unsafe.arrayBaseOffset(byte[].class);

  protected static void copyFromArray(byte[] src, int srcOffset, int length, long destAddress) {
    long offset = BYTE_ARRAY_BASE_OFFSET + srcOffset;
    _unsafe.copyMemory(src, offset, null, destAddress, length);
  }

  protected static void copyToArray(long srcAddress, byte[] dst, int dstOffset, int length) {
    long offset = BYTE_ARRAY_BASE_OFFSET + dstOffset;
    _unsafe.copyMemory(null, srcAddress, dst, offset, length);
  }

  public UnsafeCacheValue(int length) {
    super(length);
    _offHeapMemorySize.addAndGet(_length);
  }


  protected static long resolveAddress(long address, int position) {
    return address + position;
  }

  @Override
  public final void release() {
    if (!_released) {
      releaseInternal();
      _released = true;
      _offHeapMemorySize.addAndGet(0 - _length);
    }
  }

  protected abstract void releaseInternal();

}
