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

import static org.apache.blur.metrics.MetricsConstants.CACHE;
import static org.apache.blur.metrics.MetricsConstants.ENTRIES;
import static org.apache.blur.metrics.MetricsConstants.EVICTION;
import static org.apache.blur.metrics.MetricsConstants.HIT;
import static org.apache.blur.metrics.MetricsConstants.MISS;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.REMOVAL;
import static org.apache.blur.metrics.MetricsConstants.SIZE;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.blockcache_v2.cachevalue.DetachableCacheValue;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.store.IOContext;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

public class BaseCache extends Cache implements Closeable {

  private static final Log LOG = LogFactory.getLog(BaseCache.class);
  private static final long _1_MINUTE = TimeUnit.MINUTES.toMillis(1);
  protected static final long _10_SECOND = TimeUnit.SECONDS.toMillis(10);

  public enum STORE {
    ON_HEAP, OFF_HEAP
  }

  class BaseCacheEvictionListener implements EvictionListener<CacheKey, CacheValue> {
    @Override
    public void onEviction(CacheKey key, CacheValue value) {
      _evictions.mark();
      _cacheValueBufferPool.returnToPool(value.detachFromCache());
    }
  }

  protected static class BaseCacheWeigher implements Weigher<CacheValue> {
    @Override
    public int weightOf(CacheValue value) {
      try {
        return value.length();
      } catch (EvictionException e) {
        return 0;
      }
    }
  }

  private final ConcurrentLinkedHashMap<CacheKey, CacheValue> _cacheMap;
  private final FileNameFilter _readFilter;
  private final FileNameFilter _writeFilter;
  private final Size _cacheBlockSize;
  private final Size _fileBufferSize;
  private final Size _directLocalCacheRefLimit;
  private final Map<FileIdKey, Long> _fileNameToId = new ConcurrentHashMap<FileIdKey, Long>();
  private final AtomicLong _fileId = new AtomicLong();
  private final Quiet _quiet;
  private final MeterWrapper _hits;
  private final MeterWrapper _misses;
  private final MeterWrapper _evictions;
  private final MeterWrapper _removals;
  private final Thread _oldFileDaemonThread;
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final BaseCacheValueBufferPool _cacheValueBufferPool;

  public BaseCache(long totalNumberOfBytes, Size fileBufferSize, Size cacheBlockSize, Size directLocalCacheRefLimit,
      FileNameFilter readFilter, FileNameFilter writeFilter, Quiet quiet, BaseCacheValueBufferPool cacheValueBufferPool) {
    _cacheMap = new ConcurrentLinkedHashMap.Builder<CacheKey, CacheValue>().weigher(new BaseCacheWeigher())
        .maximumWeightedCapacity(totalNumberOfBytes).listener(new BaseCacheEvictionListener()).build();
    _fileBufferSize = fileBufferSize;
    _readFilter = readFilter;
    _writeFilter = writeFilter;
    _cacheBlockSize = cacheBlockSize;
    _directLocalCacheRefLimit = directLocalCacheRefLimit;
    _quiet = quiet;
    _hits = MeterWrapper.wrap(Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE, HIT), HIT, TimeUnit.SECONDS));
    _misses = MeterWrapper.wrap(Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE, MISS), MISS, TimeUnit.SECONDS));
    _evictions = MeterWrapper.wrap(Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE, EVICTION), EVICTION,
        TimeUnit.SECONDS));
    _removals = MeterWrapper.wrap(Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE, REMOVAL), REMOVAL,
        TimeUnit.SECONDS));
    _cacheValueBufferPool = cacheValueBufferPool;
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, CACHE, ENTRIES), new Gauge<Long>() {
      @Override
      public Long value() {
        return (long) getEntryCount();
      }
    });
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, CACHE, SIZE), new Gauge<Long>() {
      @Override
      public Long value() {
        return getWeightedSize();
      }
    });
    _oldFileDaemonThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          cleanupOldFiles();
          try {
            Thread.sleep(_1_MINUTE);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    });
    _oldFileDaemonThread.setDaemon(true);
    _oldFileDaemonThread.setName("BaseCacheOldFileCleanup");
    _oldFileDaemonThread.setPriority(Thread.MIN_PRIORITY);
    _oldFileDaemonThread.start();
  }

  public MeterWrapper getHitsMeter() {
    return _hits;
  }

  public int getEntryCount() {
    return _cacheMap.size();
  }

  public long getWeightedSize() {
    return _cacheMap.weightedSize();
  }

  protected void cleanupOldFiles() {
    LOG.debug("Cleanup old files from cache.");
    Set<Long> validFileIds = new HashSet<Long>(_fileNameToId.values());
    for (CacheKey key : _cacheMap.keySet()) {
      long fileId = key.getFileId();
      if (!validFileIds.contains(fileId)) {
        CacheValue remove = _cacheMap.remove(key);
        if (remove != null) {
          _removals.mark();
          _cacheValueBufferPool.returnToPool(remove.detachFromCache());
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    closeCachMap();
    _oldFileDaemonThread.interrupt();
    _cacheValueBufferPool.close();
    IOUtils.closeQuietly(_evictions);
    IOUtils.closeQuietly(_hits);
    IOUtils.closeQuietly(_misses);
    IOUtils.closeQuietly(_removals);
  }

  private void closeCachMap() {
    Collection<CacheValue> values = _cacheMap.values();
    for (CacheValue cacheValue : values) {
      cacheValue.release();
    }
    _cacheMap.clear();
  }

  @Override
  public boolean shouldBeQuiet(CacheDirectory directory, String fileName) {
    return _quiet.shouldBeQuiet(directory, fileName);
  }

  @Override
  public CacheValue newInstance(CacheDirectory directory, String fileName, int cacheBlockSize) {
    return new DetachableCacheValue(_cacheValueBufferPool.getCacheValue(cacheBlockSize));
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

  @Override
  public void fileClosedForWriting(CacheDirectory directory, String fileName, long fileId) throws IOException {
    if (directory.fileExists(fileName)) {
      long fileModified = directory.getFileModified(fileName);
      FileIdKey oldKey = new FileIdKey(directory.getDirectoryName(), fileName, -1L);
      FileIdKey newKey = new FileIdKey(directory.getDirectoryName(), fileName, fileModified);
      Long currentFileIdObject = _fileNameToId.get(oldKey);
      if (currentFileIdObject != null) {
        long currentFileId = currentFileIdObject;
        if (fileId != currentFileId) {
          throw new IOException("Something has gone very wrong file ids do not match [" + fileId + "] ["
              + currentFileId + "] for key [" + oldKey + "]");
        }
        _fileNameToId.put(newKey, fileId);
        _fileNameToId.remove(oldKey);
      }
    } else {
      throw new FileNotFoundException("File [" + fileName + "] not found in directory [" + directory + "]");
    }
  }

  private FileIdKey getCacheFileName(CacheDirectory directory, String fileName) throws IOException {
    if (directory.fileExists(fileName)) {
      long fileModified = directory.getFileModified(fileName);
      return new FileIdKey(directory.getDirectoryName(), fileName, fileModified);
    }
    return new FileIdKey(directory.getDirectoryName(), fileName, -1L);
  }

  @Override
  public int getCacheBlockSize(CacheDirectory directory, String fileName) {
    return _cacheBlockSize.getSize(directory, fileName);
  }

  @Override
  public int getFileBufferSize(CacheDirectory directory, String fileName) {
    return _fileBufferSize.getSize(directory, fileName);
  }

  @Override
  public boolean cacheFileForReading(CacheDirectory directory, String fileName, IOContext context) {
    return _readFilter.accept(directory, fileName);
  }

  @Override
  public boolean cacheFileForWriting(CacheDirectory directory, String fileName, IOContext context) {
    return _writeFilter.accept(directory, fileName);
  }

  @Override
  public CacheValue get(CacheDirectory directory, String fileName, CacheKey key) {
    CacheValue cacheValue = _cacheMap.get(key);
    if (cacheValue == null) {
      _misses.mark();
      // System.out.println("Loud Miss [" + fileName + "] Key [" + key + "]");
    } else {
      _hits.mark();
    }
    return cacheValue;
  }

  @Override
  public CacheValue getQuietly(CacheDirectory directory, String fileName, CacheKey key) {
    CacheValue cacheValue = _cacheMap.getQuietly(key);
    if (cacheValue != null) {
      _hits.mark();
    } else {
      // System.out.println("Quiet Miss [" + fileName + "] Key [" + key + "]");
    }
    return cacheValue;
  }

  @Override
  public void put(CacheDirectory directory, String fileName, CacheKey key, CacheValue value) {
    CacheValue cacheValue = _cacheMap.put(key, value);
    if (cacheValue != null) {
      _evictions.mark();
      _cacheValueBufferPool.returnToPool(cacheValue.detachFromCache());
    }
  }

  @Override
  public void releaseDirectory(CacheDirectory directory) {
    Set<Entry<FileIdKey, Long>> entrySet = _fileNameToId.entrySet();
    Iterator<Entry<FileIdKey, Long>> iterator = entrySet.iterator();
    while (iterator.hasNext()) {
      Entry<FileIdKey, Long> entry = iterator.next();
      FileIdKey fileIdKey = entry.getKey();
      if (fileIdKey._directoryName.equals(directory.getDirectoryName())) {
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

  @Override
  public IndexInputCache createIndexInputCache(CacheDirectory directory, String fileName, long fileLength) {
    int cacheBlockSize = getCacheBlockSize(directory, fileName);
    int limit = _directLocalCacheRefLimit.getSize(directory, fileName);
    if (fileLength > limit) {
      int entries = limit / cacheBlockSize;
      return new LRUIndexInputCache(fileLength, cacheBlockSize, entries, _hits);
    } else {
      return new DirectIndexInputCache(fileLength, cacheBlockSize, _hits);
    }
  }

}
