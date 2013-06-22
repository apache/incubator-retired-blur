package org.apache.blur.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;

public class MemoryReporter extends AbstractPollingReporter implements
    MetricProcessor<ConcurrentMap<String, org.apache.blur.thrift.generated.Metric>> {

  private static final Log LOG = LogFactory.getLog(MemoryReporter.class);

  private static ConcurrentMap<String, org.apache.blur.thrift.generated.Metric> _metrics = new ConcurrentHashMap<String, org.apache.blur.thrift.generated.Metric>();

  public static Map<String, org.apache.blur.thrift.generated.Metric> getMetrics() {
    return new HashMap<String, org.apache.blur.thrift.generated.Metric>(_metrics);
  }

  public static void enable() {
    MemoryReporter memoryReporter = new MemoryReporter(Metrics.defaultRegistry(), "memory-reporter");
    memoryReporter.start(1, TimeUnit.SECONDS);
  }

  protected MemoryReporter(MetricsRegistry registry, String name) {
    super(registry, name);
  }

  @Override
  public void run() {
    try {
      MetricsRegistry registry = getMetricsRegistry();
      Map<MetricName, Metric> allMetrics = registry.allMetrics();
      for (Entry<MetricName, Metric> entry : allMetrics.entrySet()) {
        entry.getValue().processWith(this, entry.getKey(), _metrics);
      }
    } catch (Exception e) {
      LOG.error("Unknown error during metrics processing.", e);
    }
  }

  @Override
  public void processMeter(MetricName name, Metered meter,
      ConcurrentMap<String, org.apache.blur.thrift.generated.Metric> context) throws Exception {
    org.apache.blur.thrift.generated.Metric metric = getMetric(name, context);
    addMeter(metric, meter, context);
  }

  private org.apache.blur.thrift.generated.Metric getMetric(MetricName name,
      ConcurrentMap<String, org.apache.blur.thrift.generated.Metric> context) {
    String nameStr = name.toString();
    org.apache.blur.thrift.generated.Metric metric = context.get(nameStr);
    if (metric == null) {
      metric = new org.apache.blur.thrift.generated.Metric();
      context.put(nameStr, metric);
      metric.setName(nameStr);
    }
    return metric;
  }

  private void addMeter(org.apache.blur.thrift.generated.Metric metric, Metered meter,
      ConcurrentMap<String, org.apache.blur.thrift.generated.Metric> context) {
    metric.putToStrMap("rateUnit", meter.rateUnit().toString());
    metric.putToStrMap("eventType", meter.eventType());
    metric.putToLongMap("count", meter.count());
    metric.putToDoubleMap("meanRate", meter.meanRate());
    metric.putToDoubleMap("oneMinuteRate", meter.oneMinuteRate());
    metric.putToDoubleMap("fiveMinuteRate", meter.fiveMinuteRate());
    metric.putToDoubleMap("fifteenMinuteRate", meter.fifteenMinuteRate());
  }

  @Override
  public void processCounter(MetricName name, Counter counter,
      ConcurrentMap<String, org.apache.blur.thrift.generated.Metric> context) throws Exception {
    org.apache.blur.thrift.generated.Metric metric = getMetric(name, context);
    metric.putToLongMap("value", counter.count());
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram,
      ConcurrentMap<String, org.apache.blur.thrift.generated.Metric> context) throws Exception {
    org.apache.blur.thrift.generated.Metric metric = getMetric(name, context);
    metric.putToDoubleMap("min", histogram.min());
    metric.putToDoubleMap("max", histogram.max());
    metric.putToDoubleMap("mean", histogram.mean());
    metric.putToDoubleMap("stdDev", histogram.stdDev());

    Snapshot snapshot = histogram.getSnapshot();
    metric.putToDoubleMap("median", snapshot.getMedian());
    metric.putToDoubleMap("75%", snapshot.get75thPercentile());
    metric.putToDoubleMap("95%", snapshot.get95thPercentile());
    metric.putToDoubleMap("98%", snapshot.get98thPercentile());
    metric.putToDoubleMap("99%", snapshot.get99thPercentile());
    metric.putToDoubleMap("99.9%", snapshot.get999thPercentile());
  }

  @Override
  public void processTimer(MetricName name, Timer timer,
      ConcurrentMap<String, org.apache.blur.thrift.generated.Metric> context) throws Exception {

    org.apache.blur.thrift.generated.Metric metric = getMetric(name, context);
    addMeter(metric, timer, context);
    metric.putToStrMap("unit", timer.durationUnit().toString());
    metric.putToDoubleMap("min", timer.min());
    metric.putToDoubleMap("max", timer.max());
    metric.putToDoubleMap("mean", timer.mean());
    metric.putToDoubleMap("stdDev", timer.stdDev());

    Snapshot snapshot = timer.getSnapshot();
    metric.putToDoubleMap("median", snapshot.getMedian());
    metric.putToDoubleMap("75%", snapshot.get75thPercentile());
    metric.putToDoubleMap("95%", snapshot.get95thPercentile());
    metric.putToDoubleMap("98%", snapshot.get98thPercentile());
    metric.putToDoubleMap("99%", snapshot.get99thPercentile());
    metric.putToDoubleMap("99.9%", snapshot.get999thPercentile());
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge,
      ConcurrentMap<String, org.apache.blur.thrift.generated.Metric> context) throws Exception {
    org.apache.blur.thrift.generated.Metric metric = getMetric(name, context);
    metric.putToDoubleMap("value", getDouble(gauge.value()));
  }

  private double getDouble(Object value) {
    if (value instanceof Integer) {
      Integer v = (Integer) value;
      return (int) v;
    } else if (value instanceof Long) {
      Long v = (Long) value;
      return (long) v;
    } else if (value instanceof Double) {
      Double v = (Double) value;
      return v;
    } else if (value instanceof Float) {
      Float v = (Float) value;
      return (float) v;
    }
    return 0;
  }

}
