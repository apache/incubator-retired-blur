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
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.blockcache_v2.cachevalue.ByteArrayCacheValue;
import org.apache.blur.store.blockcache_v2.cachevalue.UnsafeCacheValue;
import org.apache.lucene.store.IOContext;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class BaseCache extends Cache implements Closeable {

  private static final Log LOG = LogFactory.getLog(BaseCache.class);
  private static final long _1_MINUTE = TimeUnit.MINUTES.toMillis(1);
  protected static final long _1_SECOND = TimeUnit.SECONDS.toMillis(1);

  public enum STORE {
    ON_HEAP, OFF_HEAP
  }

  class BaseCacheEvictionListener implements EvictionListener<CacheKey, CacheValue> {
    @Override
    public void onEviction(CacheKey key, CacheValue value) {
      _evictions.mark();
      addToReleaseQueue(key, value);
    }
  }

  class BaseCacheWeigher implements Weigher<CacheValue> {
    @Override
    public int weightOf(CacheValue value) {
      return value.size();
    }
  }

  static class ReleaseEntry {
    CacheKey _key;
    CacheValue _value;
    final long _createTime = System.currentTimeMillis();

    @Override
    public String toString() {
      return "ReleaseEntry [_key=" + _key + ", _value=" + _value + "]";
    }

    public boolean hasLivedToLong(long warningTimeForEntryCleanup) {
      long now = System.currentTimeMillis();
      if (_createTime + warningTimeForEntryCleanup < now) {
        return true;
      }
      return false;
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
  private final Quiet _quiet;
  private final Meter _hits;
  private final Meter _misses;
  private final Meter _evictions;
  private final Meter _removals;
  private final Thread _oldFileDaemonThread;
  private final Thread _oldCacheValueDaemonThread;
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final BlockingQueue<ReleaseEntry> _releaseQueue;
  private final long _warningTimeForEntryCleanup = TimeUnit.MINUTES.toMillis(1);

  public BaseCache(long totalNumberOfBytes, Size fileBufferSize, Size cacheBlockSize, FileNameFilter readFilter,
      FileNameFilter writeFilter, Quiet quiet, STORE store) {
    _cacheMap = new ConcurrentLinkedHashMap.Builder<CacheKey, CacheValue>().weigher(new BaseCacheWeigher())
        .maximumWeightedCapacity(totalNumberOfBytes).listener(new BaseCacheEvictionListener()).build();
    _fileBufferSize = fileBufferSize;
    _readFilter = readFilter;
    _writeFilter = writeFilter;
    _store = store;
    _cacheBlockSize = cacheBlockSize;
    _quiet = quiet;
    _hits = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE, HIT), HIT, TimeUnit.SECONDS);
    _misses = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE, MISS), MISS, TimeUnit.SECONDS);
    _evictions = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE, EVICTION), EVICTION, TimeUnit.SECONDS);
    _removals = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE, REMOVAL), REMOVAL, TimeUnit.SECONDS);
    _releaseQueue = new LinkedBlockingQueue<ReleaseEntry>();
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, CACHE, ENTRIES), new Gauge<Long>() {
      @Override
      public Long value() {
        return (long) _cacheMap.size();
      }
    });
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, CACHE, SIZE), new Gauge<Long>() {
      @Override
      public Long value() {
        return _cacheMap.weightedSize();
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

    _oldCacheValueDaemonThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          cleanupOldCacheValues();
          try {
            Thread.sleep(_1_SECOND);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    });
    _oldCacheValueDaemonThread.setDaemon(true);
    _oldCacheValueDaemonThread.setName("BaseCacheCleanupCacheValues");
    _oldCacheValueDaemonThread.start();
  }

  protected void cleanupOldCacheValues() {
    Iterator<ReleaseEntry> iterator = _releaseQueue.iterator();
    while (iterator.hasNext()) {
      ReleaseEntry entry = iterator.next();
      CacheValue value = entry._value;
      if (value.refCount() == 0) {
        value.release();
        iterator.remove();
        long capacity = _cacheMap.capacity();
        _cacheMap.setCapacity(capacity + value.size());
      } else if (entry.hasLivedToLong(_warningTimeForEntryCleanup)) {
        LOG.warn("CacheValue has not been released [{0}] for over [{1} ms]", entry, _warningTimeForEntryCleanup);
      }
    }
  }

  protected void cleanupOldFiles() {
    LOG.debug("Cleanup old files from cache.");
    Set<Long> validFileIds = new HashSet<Long>(_fileNameToId.values());
    for (CacheKey key : _cacheMap.keySet()) {
      long fileId = key.getFileId();
      if (validFileIds.contains(fileId)) {
        CacheValue remove = _cacheMap.remove(key);
        if (remove != null) {
          _removals.mark();
          addToReleaseQueue(key, remove);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    _cacheMap.clear();
    _oldFileDaemonThread.interrupt();
    _oldCacheValueDaemonThread.interrupt();
    for (ReleaseEntry entry : _releaseQueue) {
      entry._value.release();
    }
  }

  private void addToReleaseQueue(CacheKey key, CacheValue value) {
    if (value != null) {
      if (value.refCount() == 0) {
        value.release();
        return;
      }
      long capacity = _cacheMap.capacity();
      _cacheMap.setCapacity(capacity - value.size());

      ReleaseEntry releaseEntry = new ReleaseEntry();
      releaseEntry._key = key;
      releaseEntry._value = value;

      LOG.info("CacheValue was not released [{0}]", releaseEntry);
      _releaseQueue.add(releaseEntry);
    }
  }

  @Override
  public boolean shouldBeQuiet(CacheDirectory directory, String fileName) {
    return _quiet.shouldBeQuiet(directory, fileName);
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
  public CacheValue get(CacheKey key) {
    CacheValue cacheValue = _cacheMap.get(key);
    if (cacheValue == null) {
      _misses.mark();
    } else {
      _hits.mark();
    }
    return cacheValue;
  }

  @Override
  public CacheValue getQuietly(CacheKey key) {
    CacheValue cacheValue = _cacheMap.getQuietly(key);
    if (cacheValue != null) {
      _hits.mark();
    }
    return cacheValue;
  }

  @Override
  public void put(CacheKey key, CacheValue value) {
    CacheValue cacheValue = _cacheMap.put(key, value);
    if (cacheValue != null) {
      _evictions.mark();
    }
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
