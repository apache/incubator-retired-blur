package org.apache.blur.metrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
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

public class HDFSReporter extends AbstractPollingReporter implements MetricProcessor<HDFSReporter.Context> {

  private static Log LOG = LogFactory.getLog(HDFSReporter.class);

  static class Context {

    private final Path path;
    private final SimpleDateFormat formatter;
    private final String name;
    private final FileSystem fileSystem;
    private String currentOutputFilePattern;
    private long now;
    private PrintWriter printStream;
    private FSDataOutputStream outputStream;
    private Path currentOutputPath;
    private long maxTimeToKeep;

    public Context(Path path, Configuration configuration, String filePattern, String name) throws IOException {
      this.path = path;
      this.fileSystem = path.getFileSystem(configuration);
      if (fileSystem.exists(path)) {
        if (!fileSystem.getFileStatus(path).isDir()) {
          throw new IOException("Path [" + path + "] is not a directory.");
        }
      } else {
        fileSystem.mkdirs(path);
      }
      this.name = name;
      this.formatter = new SimpleDateFormat(filePattern);
      this.maxTimeToKeep = TimeUnit.MINUTES.toMillis(10);
    }

    public void open(long now) throws IOException {
      this.now = now;
      String outputFilePattern = formatter.format(new Date(now));
      if (!outputFilePattern.equals(currentOutputFilePattern)) {
        // roll file
        rollFile(outputFilePattern);
        cleanupOldMetrics();
      }
    }

    private void cleanupOldMetrics() throws IOException {
      FileStatus[] listStatus = fileSystem.listStatus(path);
      for (FileStatus fileStatus : listStatus) {
        Path filePath = fileStatus.getPath();
        String fileName = filePath.getName();
        if (fileName.startsWith(name + ".")) {
          int sIndex = fileName.indexOf('.');
          int eIndex = fileName.indexOf('.', sIndex + 1);
          String pattern;
          if (eIndex < 0) {
            pattern = fileName.substring(sIndex + 1);
          } else {
            pattern = fileName.substring(sIndex + 1, eIndex);
          }
          Date date;
          try {
            date = formatter.parse(pattern);
          } catch (ParseException e) {
            throw new IOException(e);
          }
          if (date.getTime() + maxTimeToKeep < now) {
            fileSystem.delete(filePath, false);
          }
        }
      }
    }

    private void rollFile(String newOutputFilePattern) throws IOException {
      if (printStream != null) {
        printStream.close();
      }
      currentOutputPath = new Path(path, name + "." + newOutputFilePattern);
      if (fileSystem.exists(currentOutputPath)) {
        // try to append
        try {
          outputStream = fileSystem.append(currentOutputPath);
        } catch (IOException e) {
          currentOutputPath = new Path(path, name + "." + newOutputFilePattern + "." + now);
          outputStream = fileSystem.create(currentOutputPath);
        }
      } else {
        outputStream = fileSystem.create(currentOutputPath);
      }
      printStream = new PrintWriter(outputStream);
      currentOutputFilePattern = newOutputFilePattern;
    }

    public void write(JSONObject jsonObject) throws JSONException {
      jsonObject.put("timestamp", now);
      printStream.println(jsonObject.toString());
    }

    public void flush() throws IOException {
      printStream.flush();
      outputStream.flush();
      outputStream.sync();
    }
  }

  public static void enable(Configuration configuration, Path path, String filePattern, String name, long period,
      TimeUnit unit) throws IOException {
    enable(Metrics.defaultRegistry(), configuration, path, filePattern, name, period, unit);
  }

  public static void enable(MetricsRegistry metricsRegistry, Configuration configuration, Path path,
      String filePattern, String name, long period, TimeUnit unit) throws IOException {
    final HDFSReporter reporter = new HDFSReporter(metricsRegistry, configuration, path, filePattern, name);
    reporter.start(period, unit);
  }

  private final Context context;
  private final Clock clock;

  public HDFSReporter(Configuration configuration, Path path, String filePattern, String name) throws IOException {
    this(Metrics.defaultRegistry(), configuration, path, filePattern, name);
  }

  public HDFSReporter(MetricsRegistry metricsRegistry, Configuration configuration, Path path, String filePattern,
      String name) throws IOException {
    this(metricsRegistry, configuration, path, filePattern, name, Clock.defaultClock());
  }

  public HDFSReporter(MetricsRegistry metricsRegistry, Configuration configuration, Path path, String filePattern,
      String name, Clock clock) throws IOException {
    super(metricsRegistry, "hdfs-reporter");
    this.context = new Context(path, configuration, filePattern, name);
    this.clock = clock;
  }

  @Override
  public void run() {
    try {
      System.out.println("running");
      context.open(clock.time());
      for (Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics().entrySet()) {
        for (Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
          subEntry.getValue().processWith(this, subEntry.getKey(), context);
        }
      }
      context.flush();
    } catch (Throwable t) {
      LOG.error("Unknown error during the processing of metrics.", t);
    }
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, HDFSReporter.Context context) {
    JSONObject jsonObject = new JSONObject();
    try {
      jsonObject.put("name", getName(name));
      jsonObject.put("type", "gauge");
      jsonObject.put("value", gauge.value());
      context.write(jsonObject);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processCounter(MetricName name, Counter counter, HDFSReporter.Context context) {
    JSONObject jsonObject = new JSONObject();
    try {
      jsonObject.put("name", getName(name));
      jsonObject.put("type", "counter");
      jsonObject.put("value", counter.count());
      context.write(jsonObject);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processMeter(MetricName name, Metered meter, HDFSReporter.Context context) {
    JSONObject jsonObject = new JSONObject();
    try {
      jsonObject.put("name", getName(name));
      jsonObject.put("type", "meter");
      JSONObject meterJsonObject = new JSONObject();

      addMeterInfo(meter, meterJsonObject);

      jsonObject.put("value", meterJsonObject);

      context.write(jsonObject);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  private void addMeterInfo(Metered meter, JSONObject meterJsonObject) throws JSONException {
    meterJsonObject.put("rateUnit", meter.rateUnit());
    meterJsonObject.put("eventType", meter.eventType());
    meterJsonObject.put("count", meter.count());
    meterJsonObject.put("meanRate", meter.meanRate());
    meterJsonObject.put("oneMinuteRate", meter.oneMinuteRate());
    meterJsonObject.put("fiveMinuteRate", meter.fiveMinuteRate());
    meterJsonObject.put("fifteenMinuteRate", meter.fifteenMinuteRate());
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, HDFSReporter.Context context) {
    JSONObject jsonObject = new JSONObject();
    try {
      jsonObject.put("name", getName(name));
      jsonObject.put("type", "meter");
      JSONObject histogramJsonObject = new JSONObject();

      histogramJsonObject.put("min", histogram.min());
      histogramJsonObject.put("max", histogram.max());
      histogramJsonObject.put("mean", histogram.mean());
      histogramJsonObject.put("stdDev", histogram.stdDev());

      Snapshot snapshot = histogram.getSnapshot();
      JSONObject snapshotJsonObject = new JSONObject();
      snapshotJsonObject.put("median", snapshot.getMedian());
      snapshotJsonObject.put("75%", snapshot.get75thPercentile());
      snapshotJsonObject.put("95%", snapshot.get95thPercentile());
      snapshotJsonObject.put("98%", snapshot.get98thPercentile());
      snapshotJsonObject.put("99%", snapshot.get99thPercentile());
      snapshotJsonObject.put("99.9%", snapshot.get999thPercentile());

      histogramJsonObject.put("snapshot", snapshotJsonObject);

      jsonObject.put("value", histogramJsonObject);
      context.write(jsonObject);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processTimer(MetricName name, Timer timer, HDFSReporter.Context context) {
    JSONObject jsonObject = new JSONObject();
    try {
      jsonObject.put("name", getName(name));
      jsonObject.put("type", "meter");
      JSONObject timerJsonObject = new JSONObject();

      timerJsonObject.put("unit", timer.durationUnit());
      timerJsonObject.put("min", timer.min());
      timerJsonObject.put("max", timer.max());
      timerJsonObject.put("mean", timer.mean());
      timerJsonObject.put("stdDev", timer.stdDev());
      addMeterInfo(timer, timerJsonObject);

      Snapshot snapshot = timer.getSnapshot();
      JSONObject snapshotJsonObject = new JSONObject();
      snapshotJsonObject.put("median", snapshot.getMedian());
      snapshotJsonObject.put("75%", snapshot.get75thPercentile());
      snapshotJsonObject.put("95%", snapshot.get95thPercentile());
      snapshotJsonObject.put("98%", snapshot.get98thPercentile());
      snapshotJsonObject.put("99%", snapshot.get99thPercentile());
      snapshotJsonObject.put("99.9%", snapshot.get999thPercentile());

      timerJsonObject.put("snapshot", snapshotJsonObject);

      jsonObject.put("value", timerJsonObject);

      context.write(jsonObject);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
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
