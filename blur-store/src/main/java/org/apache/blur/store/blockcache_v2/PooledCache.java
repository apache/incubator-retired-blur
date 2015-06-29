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
package org.apache.blur.store.blockcache_v2;

import java.io.IOException;

import org.apache.lucene.store.IOContext;

public class PooledCache extends Cache {

  private final CachePoolStrategy _cachePoolStrategy;

  public PooledCache(CachePoolStrategy cachePoolStrategy) {
    _cachePoolStrategy = cachePoolStrategy;
  }

  @Override
  public void close() throws IOException {
    _cachePoolStrategy.close();
  }

  @Override
  public CacheValue newInstance(CacheDirectory directory, String fileName, int cacheBlockSize) {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    return cache.newInstance(directory, fileName, cacheBlockSize);
  }

  @Override
  public long getFileId(CacheDirectory directory, String fileName) throws IOException {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    return cache.getFileId(directory, fileName);
  }

  @Override
  public int getCacheBlockSize(CacheDirectory directory, String fileName) {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    return cache.getCacheBlockSize(directory, fileName);
  }

  @Override
  public int getFileBufferSize(CacheDirectory directory, String fileName) {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    return cache.getFileBufferSize(directory, fileName);
  }

  @Override
  public boolean cacheFileForReading(CacheDirectory directory, String fileName, IOContext context) {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    return cache.cacheFileForReading(directory, fileName, context);
  }

  @Override
  public boolean cacheFileForWriting(CacheDirectory directory, String fileName, IOContext context) {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    return cache.cacheFileForWriting(directory, fileName, context);
  }

  @Override
  public CacheValue get(CacheDirectory directory, String fileName, CacheKey key) {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    return cache.get(directory, fileName, key);
  }

  @Override
  public CacheValue getQuietly(CacheDirectory directory, String fileName, CacheKey key) {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    return cache.getQuietly(directory, fileName, key);
  }

  @Override
  public void put(CacheDirectory directory, String fileName, CacheKey key, CacheValue value) {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    cache.put(directory, fileName, key, value);
  }

  @Override
  public void removeFile(CacheDirectory directory, String fileName) throws IOException {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    cache.removeFile(directory, fileName);
  }

  @Override
  public void releaseDirectory(CacheDirectory directory) {
    Iterable<Cache> caches = _cachePoolStrategy.getCaches(directory);
    for (Cache cache : caches) {
      cache.releaseDirectory(directory);
    }
  }

  @Override
  public boolean shouldBeQuiet(CacheDirectory directory, String fileName) {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    return cache.shouldBeQuiet(directory, fileName);
  }

  @Override
  public void fileClosedForWriting(CacheDirectory directory, String fileName, long fileId) throws IOException {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    cache.fileClosedForWriting(directory, fileName, fileId);
  }

  public CachePoolStrategy getCachePoolStrategy() {
    return _cachePoolStrategy;
  }

  @Override
  public IndexInputCache createIndexInputCache(CacheDirectory directory, String fileName, long fileLength) {
    Cache cache = _cachePoolStrategy.getCache(directory, fileName);
    return cache.createIndexInputCache(directory, fileName, fileLength);
  }

}
