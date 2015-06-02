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

public class UnsafeAllocateCacheValue extends UnsafeCacheValue {

  private final long _address;

  public UnsafeAllocateCacheValue(int length) {
    super(length);
    _address = _unsafe.allocateMemory(_length);
  }

  @Override
  protected void writeInternal(int position, byte[] buf, int offset, int length) {
    copyFromArray(buf, offset, length, resolveAddress(_address, position));
  }

  @Override
  protected void readInternal(int position, byte[] buf, int offset, int length) {
    copyToArray(resolveAddress(_address, position), buf, offset, length);
  }

  @Override
  protected byte readInternal(int position) {
    return _unsafe.getByte(resolveAddress(_address, position));
  }

  @Override
  protected void releaseInternal() {
    _unsafe.freeMemory(_address);
  }
}
