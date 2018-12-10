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

import javax.swing.text.Position;

public interface CacheValue {

  /**
   * Detach from the base cache.
   * 
   * @return old cache value.
   */
  CacheValue detachFromCache();

  /**
   * The length of the data in this block.
   * 
   * @return the length.
   * @throws EvictionException 
   */
  int length() throws EvictionException;

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
  void read(int position, byte[] buf, int offset, int length) throws EvictionException;

  /**
   * Reads a byte from the given position.
   * 
   * @param position
   *          the position.
   * @return the byte.
   */
  byte read(int position) throws EvictionException;

  /**
   * Releases any underlying resources.
   */
  void release();

  /**
   * Reads a short from the given position.
   * 
   * @param position
   *          the {@link Position} to read from.
   * @return the short.
   */
  short readShort(int position) throws EvictionException;

  /**
   * Reads a int from the given position.
   * 
   * @param position
   *          the {@link Position} to read from.
   * @return the int.
   */
  int readInt(int position) throws EvictionException;

  /**
   * Reads a long from the given position.
   * 
   * @param position
   *          the {@link Position} to read from.
   * @return the long.
   */
  long readLong(int position) throws EvictionException;

  /**
   * This method <i>may</i> trim the existing {@link CacheValue} and produce
   * potentially a new {@link CacheValue} with the same data up to the length
   * provided. Also if a new {@link CacheValue} is produced then this method is
   * responsible to calling release on the old {@link CacheValue}.
   * 
   * @param length
   *          the valid amount of data in the {@link CacheValue}.
   * @return new trim {@link CacheValue} that has been trimmed if needed.
   */
  CacheValue trim(int length);

  boolean isEvicted();

}
