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

public abstract class UnsafeWrapperMultiCacheValue extends UnsafeCacheValue {

  private final long[] _chunkAddresses;
  private final int _chunkSize;

  public UnsafeWrapperMultiCacheValue(int length, long[] chunkAddresses, int chunkSize) {
    super(length);
    _chunkAddresses = chunkAddresses;
    _chunkSize = chunkSize;
  }

  @Override
  protected byte readInternal(int position) {
    int chunkIndex = getChunkIndex(position);
    int chunkPos = getChunkPosition(position);
    long address = _chunkAddresses[chunkIndex];
    return _unsafe.getByte(resolveAddress(address, chunkPos));
  }

  @Override
  protected void writeInternal(int position, byte[] buf, int offset, int length) {
    while (length > 0) {
      int chunkIndex = getChunkIndex(position);
      int chunkPos = getChunkPosition(position);
      int len = Math.min(length, _chunkSize - chunkPos);
      long address = _chunkAddresses[chunkIndex];
      copyFromArray(buf, offset, len, resolveAddress(address, chunkPos));
      position += len;
      offset += len;
      length -= len;
    }
  }

  @Override
  protected void readInternal(int position, byte[] buf, int offset, int length) {
    while (length > 0) {
      int chunkIndex = getChunkIndex(position);
      int chunkPos = getChunkPosition(position);
      int len = Math.min(length, _chunkSize - chunkPos);
      long address = _chunkAddresses[chunkIndex];
      copyToArray(resolveAddress(address, position), buf, offset, len);
      position += len;
      offset += len;
      length -= len;
    }
  }

  protected int getChunkPosition(int position) {
    return position % _chunkSize;
  }

  protected int getChunkIndex(int position) {
    return position / _chunkSize;
  }

}
