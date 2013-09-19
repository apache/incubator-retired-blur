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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.store.blockcache_v2.CacheValue;

@SuppressWarnings("serial")
public abstract class BaseCacheValue extends AtomicLong implements CacheValue {

  private final int _length;

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

  @Override
  public void read(int position, byte[] buf, int offset, int length) {
    if (position + length > _length) {
      throw new ArrayIndexOutOfBoundsException(position + length);
    }
    readInternal(position, buf, offset, length);
  }

  @Override
  public byte read(int position) {
    if (position >= _length) {
      throw new ArrayIndexOutOfBoundsException(position);
    }
    return readInternal(position);
  }

  protected abstract void writeInternal(int position, byte[] buf, int offset, int length);

  protected abstract byte readInternal(int position);

  protected abstract void readInternal(int position, byte[] buf, int offset, int length);

  @Override
  public final void incRef() {
    incrementAndGet();
  }

  @Override
  public final void decRef() {
    decrementAndGet();
  }

  @Override
  public final long refCount() {
    return get();
  }

}
