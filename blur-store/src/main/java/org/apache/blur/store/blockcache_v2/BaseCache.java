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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.blockcache_v2.cachevalue.ByteArrayCacheValue;
import org.apache.blur.store.blockcache_v2.cachevalue.UnsafeCacheValue;
import org.apache.lucene.store.IOContext;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;

public class BaseCache extends Cache {

  private static final Log LOG = LogFactory.getLog(BaseCache.class);

  public enum STORE {
    ON_HEAP, OFF_HEAP
  }

  class BaseCacheEvictionListener implements EvictionListener<CacheKey, CacheValue> {
    @Override
    public void onEviction(CacheKey key, CacheValue value) {
      addToReleaseQueue(value);
    }
  }

  class BaseCacheWeigher implements Weigher<CacheValue> {
    @Override
    public int weightOf(CacheValue value) {
      return value.size();
    }
  }

  private final ConcurrentLinkedHashMap<CacheKey, CacheValue> _cacheMap;
  private final FileNameFilter _readFilter;
  private final FileNameFilter _writeFilter;
  private final STORE _store;
  private final Size _cacheBlockSize;
  private final Size _fileBufferSize;
  private final Map<FileIdKey, Long> _fileNameToId = new ConcurrentHashMap<FileIdKey, Long>();
  private final AtomicLong _fileId = new AtomicLong();

  public BaseCache(long totalNumberOfBytes, Size fileBufferSize, Size cacheBlockSize, FileNameFilter readFilter,
      FileNameFilter writeFilter, STORE store) {
    _cacheMap = new ConcurrentLinkedHashMap.Builder<CacheKey, CacheValue>().weigher(new BaseCacheWeigher())
        .maximumWeightedCapacity(totalNumberOfBytes).listener(new BaseCacheEvictionListener()).build();
    _fileBufferSize = fileBufferSize;
    _readFilter = readFilter;
    _writeFilter = writeFilter;
    _store = store;
    _cacheBlockSize = cacheBlockSize;
  }

  private void addToReleaseQueue(CacheValue value) {
    if (value != null) {
      if (value.refCount() == 0) {
        value.release();
        return;
      }
      LOG.debug("CacheValue was not released [{0}]", value);
    }
  }

  @Override
  public CacheValue newInstance(CacheDirectory directory, String fileName, int cacheBlockSize) {
    switch (_store) {
    case ON_HEAP:
      return new ByteArrayCacheValue(cacheBlockSize);
    case OFF_HEAP:
      return new UnsafeCacheValue(cacheBlockSize);
    default:
      throw new RuntimeException("Unknown type [" + _store + "]");
    }
  }

  @Override
  public long getFileId(CacheDirectory directory, String fileName) throws IOException {
    FileIdKey cachedFileName = getCacheFileName(directory, fileName);
    Long id = _fileNameToId.get(cachedFileName);
    if (id != null) {
      return id;
    }
    long newId = _fileId.incrementAndGet();
    _fileNameToId.put(cachedFileName, newId);
    return newId;
  }

  @Override
  public void removeFile(CacheDirectory directory, String fileName) throws IOException {
    FileIdKey cachedFileName = getCacheFileName(directory, fileName);
    _fileNameToId.remove(cachedFileName);
  }

  private FileIdKey getCacheFileName(CacheDirectory directory, String fileName) throws IOException {
    long fileModified = directory.getFileModified(fileName);
    return new FileIdKey(directory.getDirectoryName(), fileName, fileModified);
  }

  @Override
  public int getCacheBlockSize(CacheDirectory directory, String fileName) {
    return _cacheBlockSize.getSize(directory.getDirectoryName(), fileName);
  }

  @Override
  public int getFileBufferSize(CacheDirectory directory, String fileName) {
    return _fileBufferSize.getSize(directory.getDirectoryName(), fileName);
  }

  @Override
  public boolean cacheFileForReading(CacheDirectory directory, String fileName, IOContext context) {
    return _readFilter.accept(directory.getDirectoryName(), fileName);
  }

  @Override
  public boolean cacheFileForWriting(CacheDirectory directory, String fileName, IOContext context) {
    return _writeFilter.accept(directory.getDirectoryName(), fileName);
  }

  @Override
  public CacheValue get(CacheKey key) {
    return _cacheMap.get(key);
  }

  @Override
  public void put(CacheKey key, CacheValue value) {
    _cacheMap.put(key, value);
  }

  @Override
  public void releaseDirectory(String directoryName) {
    Set<Entry<FileIdKey, Long>> entrySet = _fileNameToId.entrySet();
    Iterator<Entry<FileIdKey, Long>> iterator = entrySet.iterator();
    while (iterator.hasNext()) {
      Entry<FileIdKey, Long> entry = iterator.next();
      FileIdKey fileIdKey = entry.getKey();
      if (fileIdKey._directoryName.equals(directoryName)) {
        iterator.remove();
      }
    }
  }

  static class FileIdKey {
    final String _directoryName;
    final String _fileName;
    final long _lastModified;

    FileIdKey(String directoryName, String fileName, long lastModified) {
      _directoryName = directoryName;
      _fileName = fileName;
      _lastModified = lastModified;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((_directoryName == null) ? 0 : _directoryName.hashCode());
      result = prime * result + ((_fileName == null) ? 0 : _fileName.hashCode());
      result = prime * result + (int) (_lastModified ^ (_lastModified >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      FileIdKey other = (FileIdKey) obj;
      if (_directoryName == null) {
        if (other._directoryName != null)
          return false;
      } else if (!_directoryName.equals(other._directoryName))
        return false;
      if (_fileName == null) {
        if (other._fileName != null)
          return false;
      } else if (!_fileName.equals(other._fileName))
        return false;
      if (_lastModified != other._lastModified)
        return false;
      return true;
    }

  }

}
