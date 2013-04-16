package org.apache.blur.metrics;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.RowSortedTable;
import com.google.common.collect.Table.Cell;
import com.google.common.collect.TreeBasedTable;
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

  private static Log LOG = LogFactory.getLog(JSONReporter.class);

  private Context context;
  private Writer writer;
  private final StringBuilder jsonOutputBuilder = new StringBuilder();
  private static final AtomicReference<String> jsonData = new AtomicReference<String>("[]");

  public static void enable(String name, long period, TimeUnit unit, long ttl, TimeUnit ttlUnit) throws IOException {
    enable(Metrics.defaultRegistry(), name, period, unit, ttl, ttlUnit);
  }

  public static void enable(MetricsRegistry metricsRegistry, String name, long period, TimeUnit unit, long ttl,
      TimeUnit ttlUnit) throws IOException {
    JSONReporter reporter = new JSONReporter(metricsRegistry, name, ttl, ttlUnit);
    reporter.start(period, unit);
  }

  public static String getJSONData() {
    return jsonData.get();
  }

  protected JSONReporter(MetricsRegistry registry, String name, long ttl, TimeUnit ttlUnit) {
    super(registry, name);
    this.context = new Context(ttl, ttlUnit);
    setupBuffer();
  }

  private void setupBuffer() {
    writer = new Writer() {

      @Override
      public void write(char[] cbuf, int off, int len) throws IOException {
        jsonOutputBuilder.append(cbuf, off, len);
      }

      @Override
      public void flush() throws IOException {

      }

      @Override
      public void close() throws IOException {

      }
    };
  }

  static class Context {

    private final Map<MetricName, RowSortedTable<Long, String, Object>> numberTable;
    private final Map<MetricName, String> typeTable;
    private final long ttl;
    private Long time;

    Context(long ttl, TimeUnit ttlUnit) {
      this.numberTable = new HashMap<MetricName, RowSortedTable<Long, String, Object>>();
      this.typeTable = new HashMap<MetricName, String>();
      this.ttl = ttlUnit.toMillis(ttl);
    }

    RowSortedTable<Long, String, Object> getTable(MetricName metricName) {
      RowSortedTable<Long, String, Object> table = numberTable.get(metricName);
      if (table == null) {
        table = TreeBasedTable.create();
        numberTable.put(metricName, table);
      }
      return table;
    }

    Long getTime() {
      return time;
    }

    void setTime(Long time) {
      this.time = time;
    }

    void cleanUpOldRecords() {
      Collection<RowSortedTable<Long, String, Object>> values = numberTable.values();
      for (RowSortedTable<Long, String, Object> table : values) {
        Set<Cell<Long, String, Object>> cellSet = table.cellSet();
        Iterator<Cell<Long, String, Object>> iterator = cellSet.iterator();
        while (iterator.hasNext()) {
          Cell<Long, String, Object> cell = iterator.next();
          if (isOld(cell.getRowKey())) {
            iterator.remove();
          }
        }
      }
    }

    private boolean isOld(long rowId) {
      if (rowId + ttl < time) {
        return true;
      }
      return false;
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
      context.cleanUpOldRecords();
      jsonOutputBuilder.setLength(0);
      Set<Entry<MetricName, RowSortedTable<Long, String, Object>>> entrySet = context.numberTable.entrySet();
      jsonOutputBuilder.append('[');
      for (Entry<MetricName, RowSortedTable<Long, String, Object>> entry : entrySet) {
        if (jsonOutputBuilder.length() != 1) {
          jsonOutputBuilder.append(',');
        }
        updateJson(entry.getKey(), context.typeTable.get(entry.getKey()), entry.getValue());
      }
      jsonOutputBuilder.append(']');
      jsonData.set(jsonOutputBuilder.toString());
    } catch (Throwable t) {
      LOG.error("Unknown error during the processing of metrics.", t);
    }
  }

  private void updateJson(MetricName name, String type, RowSortedTable<Long, String, Object> table)
      throws JSONException, IOException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("name", getName(name));
    jsonObject.put("type", type);

    Set<String> columnLabels = table.columnKeySet();
    Set<Entry<Long, Map<String, Object>>> entrySet = table.rowMap().entrySet();
    JSONObject dataJsonObject = new JSONObject();

    for (Entry<Long, Map<String, Object>> row : entrySet) {
      Long rowId = row.getKey();
      Map<String, Object> cols = row.getValue();
      dataJsonObject.accumulate("timestamp", rowId);
      for (String columnLabel : columnLabels) {
        dataJsonObject.accumulate(columnLabel, cols.get(columnLabel));
      }
    }
    jsonObject.put("data", dataJsonObject);
    jsonObject.write(writer);
  }

  @Override
  public void processMeter(MetricName name, Metered meter, Context context) throws Exception {
    RowSortedTable<Long, String, Object> table = context.getTable(name);
    Long time = context.getTime();
    addMeterInfo(time, meter, table);
  }

  @Override
  public void processCounter(MetricName name, Counter counter, Context context) throws Exception {
    RowSortedTable<Long, String, Object> table = context.getTable(name);
    Long time = context.getTime();
    table.put(time, "value", counter.count());
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, Context context) throws Exception {
    RowSortedTable<Long, String, Object> table = context.getTable(name);
    Long time = context.getTime();
    table.put(time, "min", histogram.min());
    table.put(time, "max", histogram.max());
    table.put(time, "mean", histogram.mean());
    table.put(time, "stdDev", histogram.stdDev());

    Snapshot snapshot = histogram.getSnapshot();
    table.put(time, "median", snapshot.getMedian());
    table.put(time, "75%", snapshot.get75thPercentile());
    table.put(time, "95%", snapshot.get95thPercentile());
    table.put(time, "98%", snapshot.get98thPercentile());
    table.put(time, "99%", snapshot.get99thPercentile());
    table.put(time, "99.9%", snapshot.get999thPercentile());
  }

  @Override
  public void processTimer(MetricName name, Timer timer, Context context) throws Exception {
    RowSortedTable<Long, String, Object> table = context.getTable(name);
    Long time = context.getTime();
    addMeterInfo(time, timer, table);
    table.put(time, "unit", timer.durationUnit());
    table.put(time, "min", timer.min());
    table.put(time, "max", timer.max());
    table.put(time, "mean", timer.mean());
    table.put(time, "stdDev", timer.stdDev());

    Snapshot snapshot = timer.getSnapshot();
    table.put(time, "median", snapshot.getMedian());
    table.put(time, "75%", snapshot.get75thPercentile());
    table.put(time, "95%", snapshot.get95thPercentile());
    table.put(time, "98%", snapshot.get98thPercentile());
    table.put(time, "99%", snapshot.get99thPercentile());
    table.put(time, "99.9%", snapshot.get999thPercentile());
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, Context context) throws Exception {
    RowSortedTable<Long, String, Object> table = context.getTable(name);
    Long time = context.getTime();
    table.put(time, "value", gauge.value());
  }

  private void addMeterInfo(Long time, Metered meter, RowSortedTable<Long, String, Object> table) {
    table.put(time, "rateUnit", meter.rateUnit());
    table.put(time, "eventType", meter.eventType());
    table.put(time, "count", meter.count());
    table.put(time, "meanRate", meter.meanRate());
    table.put(time, "oneMinuteRate", meter.oneMinuteRate());
    table.put(time, "fiveMinuteRate", meter.fiveMinuteRate());
    table.put(time, "fifteenMinuteRate", meter.fifteenMinuteRate());
  }

  private JSONObject getName(MetricName metricName) throws JSONException {
    String group = metricName.getGroup();
    String name = metricName.getName();
    String scope = metricName.getScope();
    String type = metricName.getType();
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("name", name);
    jsonObject.put("group", group);
    jsonObject.put("scope", scope);
    jsonObject.put("type", type);
    return jsonObject;
  }

}
