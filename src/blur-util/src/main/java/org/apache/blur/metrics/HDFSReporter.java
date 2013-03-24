package org.apache.blur.metrics;

import java.io.DataOutput;
import java.io.File;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.reporting.CsvReporter;

public class HDFSReporter extends AbstractPollingReporter implements
MetricProcessor<DataOutput> {
  
  protected HDFSReporter(MetricsRegistry registry, String name) {
    super(registry, name);
  }

  public static void main(String[] args) throws InterruptedException {
    File file = new File("./target/metrics/");
    file.mkdirs();
    CsvReporter.enable(file, 1, TimeUnit.SECONDS);
    Counter counter = Metrics.newCounter(HDFSReporter.class, "test");
    while (true) {
      counter.inc();
      Thread.sleep(10);
    }
  }

  @Override
  public void processMeter(MetricName name, Metered meter, DataOutput context) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void processCounter(MetricName name, Counter counter, DataOutput context) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, DataOutput context) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void processTimer(MetricName name, Timer timer, DataOutput context) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, DataOutput context) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void run() {
    // TODO Auto-generated method stub
    
  }

}
