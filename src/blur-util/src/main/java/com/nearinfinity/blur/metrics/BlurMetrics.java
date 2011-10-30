package com.nearinfinity.blur.metrics;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;

public class BlurMetrics implements Updater {

  private MetricsRecord metricsRecord;
  
  public AtomicLong blockCacheHit = new AtomicLong(0);
  public AtomicLong blockCacheMiss = new AtomicLong(0);
  public AtomicLong blockCacheEviction = new AtomicLong(0);
  public AtomicLong rowReads = new AtomicLong(0);
  public AtomicLong rowWrites = new AtomicLong(0);
  public AtomicLong recordReads = new AtomicLong(0);
  public AtomicLong recordWrites = new AtomicLong(0);
  
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
    metricsRecord = MetricsUtil.createRecord(metricsContext, "metrics");
    metricsContext.registerUpdater(this);
  }

  @Override
  public void doUpdates(MetricsContext context) {
    synchronized (this) {
      metricsRecord.setMetric("blockcache.hit", blockCacheHit.getAndSet(0));
      metricsRecord.setMetric("blockcache.miss", blockCacheMiss.getAndSet(0));
      metricsRecord.setMetric("blockcache.eviction", blockCacheEviction.getAndSet(0));
      metricsRecord.setMetric("row.reads", rowReads.getAndSet(0));
      metricsRecord.setMetric("row.writes", rowWrites.getAndSet(0));
      metricsRecord.setMetric("record.reads", recordReads.getAndSet(0));
      metricsRecord.setMetric("record.writes", recordWrites.getAndSet(0));
    }
    metricsRecord.update();
  }

}
