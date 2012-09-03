package org.apache.blur.metrics;

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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;

public class BlurMetrics implements Updater {

  public static class MethodCall {
    public AtomicLong invokes = new AtomicLong();
    public AtomicLong times = new AtomicLong();
  }

  public AtomicLong blockCacheHit = new AtomicLong(0);
  public AtomicLong blockCacheMiss = new AtomicLong(0);
  public AtomicLong blockCacheEviction = new AtomicLong(0);
  public AtomicLong blockCacheSize = new AtomicLong(0);
  public AtomicLong rowReads = new AtomicLong(0);
  public AtomicLong rowWrites = new AtomicLong(0);
  public AtomicLong recordReads = new AtomicLong(0);
  public AtomicLong recordWrites = new AtomicLong(0);
  public AtomicLong queriesExternal = new AtomicLong(0);
  public AtomicLong queriesInternal = new AtomicLong(0);
  public AtomicLong blurShardBuffercacheAllocate1024 = new AtomicLong(0);
  public AtomicLong blurShardBuffercacheAllocate8192 = new AtomicLong(0);
  public AtomicLong blurShardBuffercacheAllocateOther = new AtomicLong(0);
  public AtomicLong blurShardBuffercacheLost = new AtomicLong(0);
  public Map<String, MethodCall> methodCalls = new ConcurrentHashMap<String, MethodCall>();

  public AtomicLong tableCount = new AtomicLong(0);
  public AtomicLong rowCount = new AtomicLong(0);
  public AtomicLong recordCount = new AtomicLong(0);
  public AtomicLong indexCount = new AtomicLong(0);
  public AtomicLong indexMemoryUsage = new AtomicLong(0);
  public AtomicLong segmentCount = new AtomicLong(0);

  private MetricsRecord _metricsRecord;
  private long _previous = System.nanoTime();

  public static void main(String[] args) throws InterruptedException {
    Configuration conf = new Configuration();
    BlurMetrics blurMetrics = new BlurMetrics(conf);
    MethodCall methodCall = new MethodCall();
    blurMetrics.methodCalls.put("test", methodCall);
    for (int i = 0; i < 100; i++) {
      blurMetrics.blockCacheHit.incrementAndGet();
      blurMetrics.blockCacheMiss.incrementAndGet();
      methodCall.invokes.incrementAndGet();
      methodCall.times.addAndGet(56000000);
      Thread.sleep(500);
    }
  }

  public BlurMetrics(Configuration conf) {
    JvmMetrics.init("blur", Long.toString(System.currentTimeMillis()));
    MetricsContext metricsContext = MetricsUtil.getContext("blur");
    _metricsRecord = MetricsUtil.createRecord(metricsContext, "metrics");
    metricsContext.registerUpdater(this);
  }

  @Override
  public void doUpdates(MetricsContext context) {
    synchronized (this) {
      long now = System.nanoTime();
      float seconds = (now - _previous) / 1000000000.0f;
      _metricsRecord.setMetric("blockcache.hit", getPerSecond(blockCacheHit.getAndSet(0), seconds));
      _metricsRecord.setMetric("blockcache.miss", getPerSecond(blockCacheMiss.getAndSet(0), seconds));
      _metricsRecord.setMetric("blockcache.eviction", getPerSecond(blockCacheEviction.getAndSet(0), seconds));
      _metricsRecord.setMetric("blockcache.size", blockCacheSize.get());
      _metricsRecord.setMetric("row.reads", getPerSecond(rowReads.getAndSet(0), seconds));
      _metricsRecord.setMetric("row.writes", getPerSecond(rowWrites.getAndSet(0), seconds));
      _metricsRecord.setMetric("record.reads", getPerSecond(recordReads.getAndSet(0), seconds));
      _metricsRecord.setMetric("record.writes", getPerSecond(recordWrites.getAndSet(0), seconds));
      _metricsRecord.setMetric("query.external", getPerSecond(queriesExternal.getAndSet(0), seconds));
      _metricsRecord.setMetric("query.internal", getPerSecond(queriesInternal.getAndSet(0), seconds));
      for (Entry<String, MethodCall> entry : methodCalls.entrySet()) {
        String key = entry.getKey();
        MethodCall value = entry.getValue();
        long invokes = value.invokes.getAndSet(0);
        long times = value.times.getAndSet(0);

        float avgTimes = (times / (float) invokes) / 1000000000.0f;
        _metricsRecord.setMetric("methodcalls." + key + ".count", getPerSecond(invokes, seconds));
        _metricsRecord.setMetric("methodcalls." + key + ".time", avgTimes);
      }
      _metricsRecord.setMetric("tables", tableCount.get());
      _metricsRecord.setMetric("rows", rowCount.get());
      _metricsRecord.setMetric("records", recordCount.get());
      _metricsRecord.setMetric("index.count", indexCount.get());
      _metricsRecord.setMetric("index.memoryusage", indexMemoryUsage.get());
      _metricsRecord.setMetric("index.segments", segmentCount.get());
      _previous = now;
    }
    _metricsRecord.update();
  }

  private float getPerSecond(long value, float seconds) {
    return (float) (value / seconds);
  }

}
