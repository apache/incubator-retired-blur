package org.apache.blur.metrics;

import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class HeapMetrics extends TimerTask {

  private final int sampleSize = (int) TimeUnit.MINUTES.toSeconds(10);
  private final Timer timer;
  private final long period = TimeUnit.SECONDS.toMillis(1);
  private final long[] heapMemoryUsageUsedHistory = new long[sampleSize];
  private final long[] heapMemoryUsageCommittedHistory = new long[sampleSize];
  private final long[] timestamp = new long[sampleSize];
  private volatile static HeapMetrics heapMetrics;
  private volatile int position = 0;

  public static synchronized HeapMetrics getInstance() {
    if (heapMetrics == null) {
      heapMetrics = new HeapMetrics();
    }
    return heapMetrics;
  }

  private HeapMetrics() {
    timer = new Timer("HeapMetrics", true);
    timer.scheduleAtFixedRate(this, period, period);
  }

  @Override
  public void run() {
    synchronized (this) {
      if (position >= sampleSize) {
        position = 0;
      }
      MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
      setValue(heapMemoryUsage.getUsed(), heapMemoryUsageUsedHistory);
      setValue(heapMemoryUsage.getCommitted(), heapMemoryUsageCommittedHistory);
      setValue(System.currentTimeMillis(), timestamp);
      position++;
    }
  }

  private void setValue(long value, long[] history) {
    history[position] = value;
  }

  public void writeJson(PrintWriter out) {
    synchronized (this) {
      out.print("{\"labels\":[{\"name\":\"used\",\"style\":{\"stroke\":\"RoyalBlue\"}},{\"name\":\"committed\",\"style\":{\"stroke\":\"Red\"}}],\"data\":[");
      int p = position;
      boolean comma = false;
      for (int i = 0; i < sampleSize; i++, p++) {
        if (p >= sampleSize) {
          p = 0;
        }
        double used = ((double) heapMemoryUsageUsedHistory[p]) / 1000000000.0;
        double committed = ((double) heapMemoryUsageCommittedHistory[p]) / 1000000000.0;
        long t = timestamp[p];
        if (t == 0) {
          continue;
        }
        if (comma) {
          out.print(",");
        }
        out.print("{\"used\":");
        out.print(used);
        out.print(",\"committed\":");
        out.print(committed);
        out.print(",\"recordTime\":");
        out.print(t);
        out.print('}');
        comma = true;
      }
      out.print("]}");
    }
  }
}
