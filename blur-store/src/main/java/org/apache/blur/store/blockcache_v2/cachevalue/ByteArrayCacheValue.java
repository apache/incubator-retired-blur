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

import org.apache.blur.store.blockcache_v2.CacheValue;

public class ByteArrayCacheValue extends BaseCacheValue {

  final byte[] _buffer;

  public ByteArrayCacheValue(int length) {
    super(length);
    _buffer = new byte[length];
  }

  @Override
  protected void writeInternal(int position, byte[] buf, int offset, int length) {
    System.arraycopy(buf, offset, _buffer, position, length);
  }

  @Override
  protected void readInternal(int position, byte[] buf, int offset, int length) {
    System.arraycopy(_buffer, position, buf, offset, length);
  }

  @Override
  protected byte readInternal(int position) {
    return _buffer[position];
  }

  @Override
  public void release() {
    _released = true;
  }

  @Override
  public CacheValue trim(int length) {
    if (_buffer.length == length) {
      return this;
    }
    ByteArrayCacheValue cacheValue = new ByteArrayCacheValue(length);
    System.arraycopy(_buffer, 0, cacheValue._buffer, 0, length);
    return cacheValue;
  }

}
