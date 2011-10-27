package com.nearinfinity.blur.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

public class BlurMetrics implements Updater {

  private MetricsRecord metricsRecord;
  private MetricsRegistry registry = new MetricsRegistry();
  
  public MetricsTimeVaryingLong blockCacheHit = new MetricsTimeVaryingLong("blockcache.cache.hit",registry);
  public MetricsTimeVaryingLong blockCacheMiss = new MetricsTimeVaryingLong("blockcache.cache.miss",registry);
  public MetricsTimeVaryingLong blockCacheEviction = new MetricsTimeVaryingLong("blockcache.eviction", registry);
  public MetricsTimeVaryingRate rowsReadRate = new MetricsTimeVaryingRate("rows.read.rate", registry);
  public MetricsTimeVaryingRate rowsWriteRate = new MetricsTimeVaryingRate("rows.written.rate", registry);
  public MetricsTimeVaryingRate recordsReadRate = new MetricsTimeVaryingRate("records.read.rate", registry);
  public MetricsTimeVaryingRate recordsWritenRate = new MetricsTimeVaryingRate("records.written.rate", registry);
  
  
  public static void main(String[] args) throws InterruptedException {
    Configuration conf = new Configuration();
    BlurMetrics blurMetrics = new BlurMetrics(conf);
    long start = System.nanoTime();
    for (int i = 0; i < 100; i++) {
      blurMetrics.blockCacheHit.inc();
      blurMetrics.blockCacheMiss.inc();
      blurMetrics.recordsReadRate.inc(1,(System.nanoTime()-start)/1000000);
      start = System.nanoTime();
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
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

}
