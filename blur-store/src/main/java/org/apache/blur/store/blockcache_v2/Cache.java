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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.store.IOContext;

public abstract class Cache implements Closeable {

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
  public CacheValue newInstance(CacheDirectory directory, String fileName) {
    return newInstance(directory, fileName, getCacheBlockSize(directory, fileName));
  }

  /**
   * Creates a new instance of CacheValue, the cache capacity should be used for
   * the given file.
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @param cacheBlockSize
   *          the length of the {@link CacheValue}.
   * @return the new CacheValue instance.
   */
  public abstract CacheValue newInstance(CacheDirectory directory, String fileName, int cacheBlockSize);

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
  public abstract long getFileId(CacheDirectory directory, String fileName) throws IOException;

  /**
   * Get capacity of each cache entry for the given file.
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @return the capacity.
   */
  public abstract int getCacheBlockSize(CacheDirectory directory, String fileName);

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
  public abstract int getFileBufferSize(CacheDirectory directory, String fileName);

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
  public abstract boolean cacheFileForReading(CacheDirectory directory, String fileName, IOContext context);

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
  public abstract boolean cacheFileForWriting(CacheDirectory directory, String fileName, IOContext context);

  /**
   * Gets the cache value for the given key. Null if missing.
   * 
   * @param key
   *          the key.
   * @return the cache value or null.
   */
  public abstract CacheValue get(CacheDirectory directory, String fileName, CacheKey key);

  /**
   * Gets the cache value for the given key. Null if missing. NOTE: This method
   * will not effect the priority of the cache.
   * 
   * @param key
   *          the key.
   * @return the cache value or null.
   */
  public abstract CacheValue getQuietly(CacheDirectory directory, String fileName, CacheKey key);

  /**
   * Puts the cache entry into the cache.
   * 
   * @param key
   *          the key.
   * @param value
   *          the value.
   */
  public abstract void put(CacheDirectory directory, String fileName, CacheKey key, CacheValue value);

  /**
   * Removes the file from the cache.
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @throws IOException
   */
  public abstract void removeFile(CacheDirectory directory, String fileName) throws IOException;

  /**
   * This is called when the CacheDirectory is finalized.
   * 
   * @param directory
   *          the directory.
   */
  public abstract void releaseDirectory(CacheDirectory directory);

  /**
   * Determines if the reader should be quiet or not.
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @return boolean.
   */
  public abstract boolean shouldBeQuiet(CacheDirectory directory, String fileName);

  /**
   * The cache internals rely on the last modified timestamp of a given file to
   * know if the file is the same or not. During the writing of a given file the
   * last moified date is not know, so this method tells the cache that the file
   * has completed the writing phase and the last modified time should now be
   * accurate.
   * 
   * @param fileId
   *          the file id.
   * @throws IOException
   */
  public abstract void fileClosedForWriting(CacheDirectory directory, String fileName, long fileId) throws IOException;

  /**
   * This method creates a local index input cache per file handle to try and
   * reduce load on the main block cache lookup system.
   * 
   * 
   * @param directory
   *          the directory.
   * @param fileName
   *          the file name.
   * @param fileLength
   *          the file length.
   * 
   * @return the IndexInputCache instance.
   */
  public abstract IndexInputCache createIndexInputCache(CacheDirectory directory, String fileName, long fileLength);

}
