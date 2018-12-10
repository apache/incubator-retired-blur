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
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.metrics.AtomicLongGauge;
import org.apache.blur.store.blockcache_v2.CacheValue;
import org.apache.blur.store.blockcache_v2.EvictionException;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;

public abstract class BaseCacheValue implements CacheValue {

  private static final AtomicLong _neededFinalizedCall = new AtomicLong();

  protected final int _length;
  protected volatile boolean _released = false;
  protected volatile boolean _evicted = false;

  static {
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, JVM, CACHE_VALUE_FINALIZE), new AtomicLongGauge(
        _neededFinalizedCall));
  }

  public BaseCacheValue(int length) {
    _length = length;
  }

  @Override
  public final int length() {
    return _length;
  }

  @Override
  public void write(int position, byte[] buf, int offset, int length) {

    if (position + length > _length) {
      throw new ArrayIndexOutOfBoundsException(position + length);
    }
    writeInternal(position, buf, offset, length);
  }

  private void checkForEviction() throws EvictionException {
    if (_evicted) {
      throw new EvictionException();
    }
  }

  @Override
  public void read(int position, byte[] buf, int offset, int length) throws EvictionException {
    checkForEviction();
    if (position + length > _length) {
      throw new ArrayIndexOutOfBoundsException(position + length);
    }
    readInternal(position, buf, offset, length);
  }

  @Override
  public byte read(int position) throws EvictionException {
    checkForEviction();
    if (position >= _length) {
      throw new ArrayIndexOutOfBoundsException(position);
    }
    return readInternal(position);
  }

  @Override
  public short readShort(int position) throws EvictionException {
    checkForEviction();
    if (position + 2 > _length) {
      throw new ArrayIndexOutOfBoundsException(position + 2);
    }
    return readShortInternal(position);
  }

  protected short readShortInternal(int position) {
    return (short) (((readInternal(position) & 0xFF) << 8) | (readInternal(position + 1) & 0xFF));
  }

  @Override
  public int readInt(int position) throws EvictionException {
    checkForEviction();
    if (position + 4 > _length) {
      throw new ArrayIndexOutOfBoundsException(position + 4);
    }
    return readIntInternal(position);
  }

  protected int readIntInternal(int position) {
    return ((readInternal(position) & 0xFF) << 24) | ((readInternal(position + 1) & 0xFF) << 16)
        | ((readInternal(position + 2) & 0xFF) << 8) | (readInternal(position + 3) & 0xFF);
  }

  @Override
  public long readLong(int position) throws EvictionException {
    checkForEviction();
    if (position + 8 > _length) {
      throw new ArrayIndexOutOfBoundsException(position + 4);
    }
    return readLongInternal(position);
  }

  protected long readLongInternal(int position) {
    return (((long) readIntInternal(position)) << 32) | (readIntInternal(position + 4) & 0xFFFFFFFFL);
  }

  protected abstract void writeInternal(int position, byte[] buf, int offset, int length);

  protected abstract byte readInternal(int position);

  protected abstract void readInternal(int position, byte[] buf, int offset, int length);

  @Override
  public CacheValue trim(int length) {
    return this;
  }

  @Override
  public CacheValue detachFromCache() {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public final boolean isEvicted() {
    return _evicted;
  }

}
