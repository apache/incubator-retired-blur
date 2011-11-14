package com.nearinfinity.blur.metrics;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;

public class BlurMetrics implements Updater {

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

  private MetricsRecord _metricsRecord;
  private long _previous = System.nanoTime();

  public static void main(String[] args) throws InterruptedException {
    Configuration conf = new Configuration();
    BlurMetrics blurMetrics = new BlurMetrics(conf);
    for (int i = 0; i < 100; i++) {
      blurMetrics.blockCacheHit.incrementAndGet();
      blurMetrics.blockCacheMiss.incrementAndGet();
      Thread.sleep(1000);
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
      double seconds = (now - _previous) / 1000000000.0;
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
      _previous = now;
    }
    _metricsRecord.update();
  }

  private long getPerSecond(long value, double seconds) {
    return (long) (value / seconds);
  }

}
