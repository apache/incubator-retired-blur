package org.apache.blur.metrics;

import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class SystemLoadMetrics extends TimerTask {

  private final int sampleSize = (int) TimeUnit.MINUTES.toSeconds(10);
  private final Timer timer;
  private final long period = TimeUnit.SECONDS.toMillis(1);
  private final double[] systemLoadAverageHistory = new double[sampleSize];
  private final long[] timestamp = new long[sampleSize];
  private volatile static SystemLoadMetrics instance;
  private volatile int position = 0;

  public static synchronized SystemLoadMetrics getInstance() {
    if (instance == null) {
      instance = new SystemLoadMetrics();
    }
    return instance;
  }

  private SystemLoadMetrics() {
    timer = new Timer("SystemLoadMetrics", true);
    timer.scheduleAtFixedRate(this, period, period);
  }

  @Override
  public void run() {
    synchronized (this) {
      if (position >= sampleSize) {
        position = 0;
      }
      OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
      systemLoadAverageHistory[position] = operatingSystemMXBean.getSystemLoadAverage();
      timestamp[position] = System.currentTimeMillis();
      position++;
    }
  }

  public void writeJson(PrintWriter out) {
    synchronized (this) {
      out.print("{\"labels\":[{\"name\":\"load\",\"style\":{\"stroke\":\"Red\"}}],\"data\":[");
      int p = position;
      boolean comma = false;
      for (int i = 0; i < sampleSize; i++, p++) {
        if (p >= sampleSize) {
          p = 0;
        }
        double load = systemLoadAverageHistory[p];
        long t = timestamp[p];
        if (t == 0) {
          continue;
        }
        if (comma) {
          out.print(",");
        }
        out.print("{\"load\":");
        out.print(load);
        out.print(",\"recordTime\":");
        out.print(t);
        out.print('}');
        comma = true;
      }
      out.print("]}");
    }
  }
}
