package org.apache.blur.metrics;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;

public class JSONReporter extends AbstractPollingReporter implements MetricProcessor<JSONReporter.Context> {

  private static final ResetableCharArrayWriter EMPTY = new ResetableCharArrayWriter() {
    {
      try {
        write("[]");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  };
  private static Log LOG = LogFactory.getLog(JSONReporter.class);

  private Context context;
  private int _256K = 262144;
  private ResetableCharArrayWriter writerInUse = new ResetableCharArrayWriter(_256K);
  private ResetableCharArrayWriter writerWriting = new ResetableCharArrayWriter(_256K);
  private static AtomicReference<ResetableCharArrayWriter> reading = new AtomicReference<ResetableCharArrayWriter>(
      EMPTY);

  public static void enable(String name, long period, TimeUnit unit, int numberOfElements) throws IOException {
    enable(Metrics.defaultRegistry(), name, period, unit, numberOfElements);
  }

  public static void enable(MetricsRegistry metricsRegistry, String name, long period, TimeUnit unit,
      int numberOfElements) throws IOException {
    JSONReporter reporter = new JSONReporter(metricsRegistry, name, numberOfElements);
    reporter.start(period, unit);
  }

  public static void writeJSONData(Writer writer) throws IOException {
    synchronized (reading) {
      ResetableCharArrayWriter reader = reading.get();
      writer.write(reader.getBuffer(), 0, reader.size());
    }
  }

  protected JSONReporter(MetricsRegistry registry, String name, int numberOfElements) {
    super(registry, name);
    this.context = new Context(numberOfElements);
  }

  static class Context {

    private final Map<MetricName, MetricInfo> metricInfoMap = new HashMap<MetricName, MetricInfo>();
    private final Map<MetricName, String> typeTable;
    private final int numberOfElements;
    private long time;

    Context(int numberOfElements) {
      this.typeTable = new HashMap<MetricName, String>();
      this.numberOfElements = numberOfElements;
    }

    long getTime() {
      return time;
    }

    void setTime(long time) {
      this.time = time;
    }

    public MetricInfo getMetricInfo(MetricName name) {
      MetricInfo info = metricInfoMap.get(name);
      if (info == null) {
        info = new MetricInfo(getName(name), typeTable.get(name), numberOfElements);
        metricInfoMap.put(name, info);
      }
      return info;
    }

    private String getName(MetricName metricName) {
      // String group = metricName.getGroup();
      // String name = metricName.getName();
      // String scope = metricName.getScope();
      // String type = metricName.getType();
      // JSONObject jsonObject = new JSONObject();
      // jsonObject.put("name", name);
      // jsonObject.put("group", group);
      // jsonObject.put("scope", scope);
      // jsonObject.put("type", type);
      return metricName.toString();
    }
  }

  @Override
  public void run() {
    try {
      context.setTime(System.currentTimeMillis());
      for (Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics().entrySet()) {
        for (Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
          MetricName name = subEntry.getKey();
          Metric metric = subEntry.getValue();
          if (metric instanceof Counter) {
            context.typeTable.put(name, "counter");
          } else if (metric instanceof Gauge) {
            context.typeTable.put(name, "gauge");
          } else if (metric instanceof Histogram) {
            context.typeTable.put(name, "histogram");
          } else if (metric instanceof Meter) {
            context.typeTable.put(name, "meter");
          } else if (metric instanceof Timer) {
            context.typeTable.put(name, "timer");
          }
          metric.processWith(this, name, context);
        }
      }
      ResetableCharArrayWriter writer = getWriter();
      writer.reset();
      Set<Entry<MetricName, MetricInfo>> entrySet = context.metricInfoMap.entrySet();
      writer.append('[');
      boolean flag = false;
      for (Entry<MetricName, MetricInfo> entry : entrySet) {
        if (flag) {
          writer.append(',');
        }
        entry.getValue().write(writer);
        flag = true;
      }
      writer.append(']');
      swapWriter();
    } catch (Throwable t) {
      LOG.error("Unknown error during the processing of metrics.", t);
    }
  }

  private void swapWriter() {
    synchronized (reading) {
      ResetableCharArrayWriter tmp1 = writerWriting;
      writerWriting = writerInUse;
      writerInUse = tmp1;
      reading.set(writerInUse);
    }
  }

  private ResetableCharArrayWriter getWriter() {
    return writerWriting;
  }

  @Override
  public void processMeter(MetricName name, Metered meter, Context context) throws Exception {
    MetricInfo info = context.getMetricInfo(name);
    long time = context.getTime();
    addMeterInfo(time, meter, info);
  }

  @Override
  public void processCounter(MetricName name, Counter counter, Context context) throws Exception {
    MetricInfo info = context.getMetricInfo(name);
    long time = context.getTime();
    info.addNumber("timestamp", time);
    info.addNumber("value", counter.count());
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, Context context) throws Exception {
    MetricInfo info = context.getMetricInfo(name);
    long time = context.getTime();
    info.addNumber("timestamp", time);
    info.addNumber("min", histogram.min());
    info.addNumber("max", histogram.max());
    info.addNumber("mean", histogram.mean());
    info.addNumber("stdDev", histogram.stdDev());

    Snapshot snapshot = histogram.getSnapshot();
    info.addNumber("median", snapshot.getMedian());
    info.addNumber("75%", snapshot.get75thPercentile());
    info.addNumber("95%", snapshot.get95thPercentile());
    info.addNumber("98%", snapshot.get98thPercentile());
    info.addNumber("99%", snapshot.get99thPercentile());
    info.addNumber("99.9%", snapshot.get999thPercentile());
  }

  @Override
  public void processTimer(MetricName name, Timer timer, Context context) throws Exception {
    MetricInfo info = context.getMetricInfo(name);
    long time = context.getTime();

    addMeterInfo(time, timer, info);
    info.setMetaData("unit", timer.durationUnit().toString());
    info.addNumber("min", timer.min());
    info.addNumber("max", timer.max());
    info.addNumber("mean", timer.mean());
    info.addNumber("stdDev", timer.stdDev());

    Snapshot snapshot = timer.getSnapshot();
    info.addNumber("median", snapshot.getMedian());
    info.addNumber("75%", snapshot.get75thPercentile());
    info.addNumber("95%", snapshot.get95thPercentile());
    info.addNumber("98%", snapshot.get98thPercentile());
    info.addNumber("99%", snapshot.get99thPercentile());
    info.addNumber("99.9%", snapshot.get999thPercentile());
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, Context context) throws Exception {
    MetricInfo info = context.getMetricInfo(name);
    long time = context.getTime();
    info.addNumber("timestamp", time);
    info.addNumber("value", getDouble(gauge.value()));
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

  private void addMeterInfo(Long time, Metered meter, MetricInfo info) {
    info.addNumber("timestamp", time);
    info.setMetaData("rateUnit", meter.rateUnit().toString());
    info.setMetaData("eventType", meter.eventType());
    info.addNumber("count", meter.count());
    info.addNumber("meanRate", meter.meanRate());
    info.addNumber("oneMinuteRate", meter.oneMinuteRate());
    info.addNumber("fiveMinuteRate", meter.fiveMinuteRate());
    info.addNumber("fifteenMinuteRate", meter.fifteenMinuteRate());
  }

}
