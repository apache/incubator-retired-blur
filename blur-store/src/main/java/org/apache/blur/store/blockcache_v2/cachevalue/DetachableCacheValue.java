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

import static org.apache.blur.metrics.MetricsConstants.CACHE;
import static org.apache.blur.metrics.MetricsConstants.DETACHES;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.blur.store.blockcache_v2.CacheValue;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

@SuppressWarnings("serial")
public class DetachableCacheValue extends AtomicInteger implements CacheValue {

  private static final Meter _detaches;

  static {
    _detaches = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE, DETACHES), DETACHES, TimeUnit.SECONDS);
  }

  private volatile CacheValue _baseCacheValue;

  public DetachableCacheValue(CacheValue cacheValue) {
    _baseCacheValue = cacheValue;
  }

  @Override
  public CacheValue detachFromCache() {
    if (_baseCacheValue instanceof ByteArrayCacheValue) {
      // already detached
      return null;
    } else if (_baseCacheValue instanceof UnsafeCacheValue) {
      final CacheValue result = _baseCacheValue;
      if (get() == 0) {
        // No one is using this so don't copy
        // NULL out reference so just in case there can't be a seg fault.
        _baseCacheValue = null;
      } else {
        // Copy data, because someone might access at some point
        _detaches.mark();
        int length = _baseCacheValue.length();
        ByteArrayCacheValue byteArrayCacheValue = new ByteArrayCacheValue(length);
        _baseCacheValue.read(0, byteArrayCacheValue._buffer, 0, length);
        _baseCacheValue = byteArrayCacheValue;
      }
      return result;
    } else {
      throw new RuntimeException("Unsupported type of [" + _baseCacheValue + "]");
    }
  }

  @Override
  public int length() {
    return _baseCacheValue.length();
  }

  @Override
  public void write(int position, byte[] buf, int offset, int length) {
    _baseCacheValue.write(position, buf, offset, length);
  }

  @Override
  public void read(int position, byte[] buf, int offset, int length) {
    _baseCacheValue.read(position, buf, offset, length);
  }

  @Override
  public byte read(int position) {
    return _baseCacheValue.read(position);
  }

  @Override
  public void release() {
    _baseCacheValue.release();
  }

  @Override
  public short readShort(int position) {
    return _baseCacheValue.readShort(position);
  }

  @Override
  public int readInt(int position) {
    return _baseCacheValue.readInt(position);
  }

  @Override
  public long readLong(int position) {
    return _baseCacheValue.readLong(position);
  }

  @Override
  public CacheValue trim(int length) {
    return this;
  }

  @Override
  public void decRef() {
    decrementAndGet();
  }

  @Override
  public void incRef() {
    incrementAndGet();
  }

}