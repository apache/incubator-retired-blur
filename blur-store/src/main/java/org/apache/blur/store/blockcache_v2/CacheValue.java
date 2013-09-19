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
package org.apache.blur.store.blockcache_v2;

public interface CacheValue {

  /**
   * The actual size of the the underlying resource.
   * 
   * @return the size.
   */
  int size();

  /**
   * The length of the data in this block.
   * 
   * @return the length.
   */
  int length();

  /**
   * Writes data out to a given position in this block.
   * 
   * @param position
   *          the position.
   * @param buf
   *          the buffer.
   * @param offset
   *          the offset in the buffer.
   * @param length
   *          the length of bytes to write.
   */
  void write(int position, byte[] buf, int offset, int length);

  /**
   * Reads data into the buffer given the position.
   * 
   * @param position
   *          the position to read.
   * @param buf
   *          the buffer to read into.
   * @param offset
   *          the offset within the buffer.
   * @param length
   *          the length of data to read.
   */
  void read(int position, byte[] buf, int offset, int length);

  /**
   * Reads a byte from the given position.
   * 
   * @param position
   *          the position.
   * @return the byte.
   */
  byte read(int position);

  /**
   * Increments the reference.
   */
  void incRef();

  /**
   * Decrements the reference.
   */
  void decRef();

  /**
   * Gets the reference count.
   */
  long refCount();

  /**
   * Releases any underlying resources.
   */
  void release();

}
