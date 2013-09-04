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

import java.io.IOException;

import org.apache.lucene.store.IOContext;

public interface Cache {

  /**
   * Creates a new instance of CacheValue, the cache capacity should be used for
   * the given file.
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @return the new CacheValue instance.
   */
  CacheValue newInstance(CacheDirectory directory, String fileName);

  /**
   * Gets unique id for the given file. This is assumed to be unique even if the
   * file is deleted and recreated.
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @return the file id.
   * @throws IOException
   */
  long getFileId(CacheDirectory directory, String fileName) throws IOException;

  /**
   * Get capacity of each cache entry for the given file.
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @return the capacity.
   */
  int getCacheBlockSize(CacheDirectory directory, String fileName);

  /**
   * Gets buffer size of the buffer used while interacting with the underlying
   * directory.
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @return the buffer size.
   */
  int getFileBufferSize(CacheDirectory directory, String fileName);

  /**
   * Checks whether file should be cached or not during reading.
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @param context
   *          the IOContext from Lucene.
   * @return boolean.
   */
  boolean cacheFileForReading(CacheDirectory directory, String fileName, IOContext context);

  /**
   * Checks whether file should be cached or not during writing.
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @param context
   *          the IOContext from Lucene.
   * @return boolean.
   */
  boolean cacheFileForWriting(CacheDirectory directory, String fileName, IOContext context);

  /**
   * Gets the cache value for the given key. Null if missing.
   * 
   * @param key
   *          the key.
   * @return the cache value or null.
   */
  CacheValue get(CacheKey key);

  /**
   * Puts the cache entry into the cache.
   * 
   * @param key
   *          the key.
   * @param value
   *          the value.
   */
  void put(CacheKey key, CacheValue value);

  /**
   * Removes the file from the cache.
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @throws IOException
   */
  void removeFile(CacheDirectory directory, String fileName) throws IOException;

  /**
   * This is called when the CacheDirectory is finalized.
   * 
   * @param directoryName
   *          the directory name.
   */
  void releaseDirectory(String directoryName);

}
