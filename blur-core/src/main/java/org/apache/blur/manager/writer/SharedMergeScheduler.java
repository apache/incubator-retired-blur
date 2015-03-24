package org.apache.blur.manager.writer;

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
import static org.apache.blur.metrics.MetricsConstants.LUCENE;
import static org.apache.blur.metrics.MetricsConstants.MERGE_THROUGHPUT_BYTES;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.utils.BlurConstants.SHARED_MERGE_SCHEDULER_PREFIX;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.store.Directory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class SharedMergeScheduler implements Closeable {

  private static final String LARGE_QUEUE_DEPTH_IN_BYTES = "Large Queue Depth In Bytes";
  private static final String LARGE_QUEUE_DEPTH = "Large Queue Depth";
  private static final String SMALL_QUEUE_DEPTH_IN_BYTES = "Small Queue Depth In Bytes";
  private static final String SMALL_QUEUE_DEPTH = "Small Queue Depth";
  private static final Log LOG = LogFactory.getLog(SharedMergeScheduler.class);
  private static final Meter _throughputBytes;

  static {
    MetricName mergeThoughputBytes = new MetricName(ORG_APACHE_BLUR, LUCENE, MERGE_THROUGHPUT_BYTES);
    _throughputBytes = Metrics.newMeter(mergeThoughputBytes, MERGE_THROUGHPUT_BYTES, TimeUnit.SECONDS);
  }

  private final AtomicBoolean _running = new AtomicBoolean(true);

  private final ExecutorService _smallMergeService;
  private final ExecutorService _largeMergeService;
  private final PriorityBlockingQueue<MergeWork> _smallMergeQueue = new PriorityBlockingQueue<MergeWork>();
  private final PriorityBlockingQueue<MergeWork> _largeMergeQueue = new PriorityBlockingQueue<MergeWork>();
  private final long _smallMergeThreshold;

  static class MergeWork implements Comparable<MergeWork> {

    private final String _id;
    private final MergePolicy.OneMerge _merge;
    private final IndexWriter _writer;
    private final long _size;

    public MergeWork(String id, OneMerge merge, IndexWriter writer) throws IOException {
      _id = id;
      _merge = merge;
      _writer = writer;
      _size = merge.totalBytesSize();
    }

    @Override
    public int compareTo(MergeWork o) {
      if (_size == o._size) {
        return 0;
      }
      return _size < o._size ? -1 : 1;
    }

    public void merge() throws IOException {
      MergeStatus mergeStatus = new MergeStatus(_merge, _writer.getDirectory(), _size, _merge.segments);
      // Trace.setupTrace(BlurConstants.SHARED_MERGE_SCHEDULER_PREFIX + "-" +
      // System.nanoTime());
      startWatching(mergeStatus);
      try {
        _writer.merge(_merge);
      } finally {
        stopWatching(mergeStatus);
        // Trace.tearDownTrace();
      }
      _throughputBytes.mark(_size);
    }

    public String getId() {
      return _id;
    }

    public long getSize() {
      return _size;
    }
  }

  static class MergeStatus {

    final String _id;
    final Directory _directory;
    final long _start;
    final long _size;
    final OneMerge _oneMerge;
    final List<SegmentInfoPerCommit> _segments;

    MergeStatus(OneMerge oneMerge, Directory directory, long size, List<SegmentInfoPerCommit> segments) {
      _id = UUID.randomUUID().toString();
      _directory = directory;
      _start = System.nanoTime();
      _size = size;
      _oneMerge = oneMerge;
      _segments = segments;
    }

    void finalReport() throws IOException {
      long e = System.nanoTime();
      double time = (e - _start) / 1000000000.0;
      double rate = (_size / 1000.0 / 1000.0) / time;
      SegmentInfo segmentInfo = getSegmentInfo(_oneMerge);
      long segmentSize = getSegmentSize(segmentInfo, _directory);
      LOG.info(
          "Merge took [{0} s] to complete at rate of [{1} MB/s], input bytes [{2}], output bytes [{4}], segments merged {3}",
          time, rate, _size, _segments, segmentSize);
    }

    void report() throws IOException {
      long e = System.nanoTime();
      double time = (e - _start) / 1000000000.0;
      SegmentInfo segmentInfo = getSegmentInfo(_oneMerge);
      long segmentSize = getSegmentSize(segmentInfo, _directory);
      double rate = (segmentSize / 1000.0 / 1000.0) / time;
      LOG.info(
          "Merge running for [{0} s] at rate of [{1} MB/s], input bytes [{2}], output bytes [{4}], segments being merged {3}",
          time, rate, _size, _segments, segmentSize);
    }

  }

  public SharedMergeScheduler(int threads) {
    this(threads, 128 * 1000 * 1000);
  }

  private static ConcurrentMap<String, MergeStatus> _mergeStatusMap = new ConcurrentHashMap<String, MergeStatus>();

  protected static void stopWatching(MergeStatus mergeStatus) throws IOException {
    MergeStatus status = _mergeStatusMap.remove(mergeStatus._id);
    status.finalReport();
  }

  protected static void startWatching(MergeStatus mergeStatus) {
    _mergeStatusMap.put(mergeStatus._id, mergeStatus);
  }

  public SharedMergeScheduler(int threads, long smallMergeThreshold) {
    MetricName mergeSmallQueueDepth = new MetricName(ORG_APACHE_BLUR, LUCENE, SMALL_QUEUE_DEPTH);
    MetricName mergeSmallQueueDepthInBytes = new MetricName(ORG_APACHE_BLUR, LUCENE, SMALL_QUEUE_DEPTH_IN_BYTES);
    MetricName mergeLargeQueueDepth = new MetricName(ORG_APACHE_BLUR, LUCENE, LARGE_QUEUE_DEPTH);
    MetricName mergeLargeQueueDepthInBytes = new MetricName(ORG_APACHE_BLUR, LUCENE, LARGE_QUEUE_DEPTH_IN_BYTES);

    _smallMergeThreshold = smallMergeThreshold;
    _smallMergeService = Executors.newThreadPool(SHARED_MERGE_SCHEDULER_PREFIX + "-small", threads, false);
    _largeMergeService = Executors.newThreadPool(SHARED_MERGE_SCHEDULER_PREFIX + "-large", threads, false);
    for (int i = 0; i < threads; i++) {
      _smallMergeService.submit(getMergerRunnable(_smallMergeQueue));
      _largeMergeService.submit(getMergerRunnable(_largeMergeQueue));
    }

    Metrics.newGauge(mergeSmallQueueDepth, new Gauge<Long>() {
      @Override
      public Long value() {
        return (long) _smallMergeQueue.size();
      }
    });
    Metrics.newGauge(mergeSmallQueueDepthInBytes, new Gauge<Long>() {
      @Override
      public Long value() {
        return getSizeInBytes(_smallMergeQueue);
      }
    });
    Metrics.newGauge(mergeLargeQueueDepth, new Gauge<Long>() {
      @Override
      public Long value() {
        return (long) _largeMergeQueue.size();
      }
    });
    Metrics.newGauge(mergeLargeQueueDepthInBytes, new Gauge<Long>() {
      @Override
      public Long value() {
        return getSizeInBytes(_largeMergeQueue);
      }
    });
  }

  protected long getSizeInBytes(PriorityBlockingQueue<MergeWork> queue) {
    long total = 0;
    for (MergeWork mergeWork : queue) {
      total += mergeWork.getSize();
    }
    return total;
  }

  private Runnable getMergerRunnable(final PriorityBlockingQueue<MergeWork> queue) {
    return new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          try {
            MergeWork mergeWork = queue.take();
            try {
              mergeWork.merge();
            } catch (Throwable t) {
              LOG.error("Unknown error while trying to perform merge on [{0}]", t, mergeWork);
            }
          } catch (InterruptedException e) {
            if (_running.get()) {
              LOG.error("Unknown error", e);
            }
            return;
          }
        }
      }
    };
  }

  public MergeScheduler getMergeScheduler() {
    return new MergeScheduler() {

      private final String _id = UUID.randomUUID().toString();

      @Override
      public void merge(IndexWriter writer) throws IOException {
        addMerges(_id, writer);
      }

      @Override
      public void close() throws IOException {
        remove(_id);
      }

      @Override
      public MergeScheduler clone() {
        return getMergeScheduler();
      }
    };
  }

  protected void addMerges(String id, IndexWriter writer) throws IOException {
    OneMerge merge;
    while ((merge = writer.getNextMerge()) != null) {
      addMerge(id, writer, merge);
    }
  }

  private void addMerge(String id, IndexWriter writer, OneMerge merge) throws IOException {
    MergeWork mergeWork = new MergeWork(id, merge, writer);
    if (isLargeMerge(merge)) {
      _largeMergeQueue.add(mergeWork);
    } else {
      _smallMergeQueue.add(mergeWork);
    }
  }

  private boolean isLargeMerge(OneMerge merge) throws IOException {
    long totalBytesSize = merge.totalBytesSize();
    if (totalBytesSize <= _smallMergeThreshold) {
      return false;
    }
    return true;
  }

  protected void remove(String id) {
    remove(_smallMergeQueue, id);
    remove(_largeMergeQueue, id);
  }

  private void remove(PriorityBlockingQueue<MergeWork> queue, String id) {
    Iterator<MergeWork> iterator = queue.iterator();
    while (iterator.hasNext()) {
      MergeWork mergeWork = iterator.next();
      if (id.equals(mergeWork.getId())) {
        iterator.remove();
      }
    }
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    _smallMergeService.shutdownNow();
    _largeMergeService.shutdownNow();
  }

  protected static long getSegmentSize(SegmentInfo newSegmentInfo, Directory directory) throws IOException {
    if (newSegmentInfo == null) {
      return -1L;
    }
    String prefix = newSegmentInfo.name;
    long total = 0;
    for (String name : directory.listAll()) {
      if (name.startsWith(prefix)) {
        total += directory.fileLength(name);
      }
    }
    return total;
  }

  protected static SegmentInfo getSegmentInfo(OneMerge oneMerge) {
    Object segmentInfoPerCommit = getFieldObject(oneMerge, "info");
    if (segmentInfoPerCommit == null) {
      return null;
    }
    return (SegmentInfo) getFieldObject(segmentInfoPerCommit, "info");
  }

  protected static Object getFieldObject(Object o, String fieldName) {
    try {
      Field field = o.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(o);
    } catch (NoSuchFieldException e) {
      return null;
    } catch (SecurityException e) {
      return null;
    } catch (IllegalArgumentException e) {
      return null;
    } catch (IllegalAccessException e) {
      return null;
    }
  }
}
