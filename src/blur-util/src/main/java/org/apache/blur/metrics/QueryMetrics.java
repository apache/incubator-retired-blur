package org.apache.blur.metrics;

import java.io.PrintWriter;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class QueryMetrics extends TimerTask {

  private final int sampleSize = (int) TimeUnit.MINUTES.toSeconds(10);
  private final AtomicLong _queryCount = new AtomicLong();
  private final AtomicLong _queryTime = new AtomicLong();
  private final AtomicLong _queryExceptionCount = new AtomicLong();
  private final AtomicLong _dataFetchCount = new AtomicLong();
  private final AtomicLong _dataFetchTime = new AtomicLong();
  private final AtomicLong _dataFetchRecordCount = new AtomicLong();
  private final AtomicLong _dataMutateCount = new AtomicLong();
  private final AtomicLong _dataMutateTime = new AtomicLong();
  private final AtomicLong _dataMutateRecordCount = new AtomicLong();
  private final Timer timer;
  private final long period = TimeUnit.SECONDS.toMillis(1);
  private final double[] _queryRate = new double[sampleSize];
  private final double[] _queryResponse = new double[sampleSize];
  private final double[] _queryExceptionRate = new double[sampleSize];
  private final double[] _fetchRecordRate = new double[sampleSize];
  private final double[] _fetchResponse = new double[sampleSize];
  private final double[] _mutateRecordRate = new double[sampleSize];
  private final double[] _mutateResponse = new double[sampleSize];
  private final long[] timestamp = new long[sampleSize];
  private volatile static QueryMetrics instance;
  private volatile int position;

  public static synchronized QueryMetrics getInstance() {
    if (instance == null) {
      instance = new QueryMetrics();
    }
    return instance;
  }

  private QueryMetrics() {
    timer = new Timer("QueryMetrics", true);
    timer.scheduleAtFixedRate(this, period, period);
  }

  @Override
  public void run() {
    synchronized (this) {
      if (position >= sampleSize) {
        position = 0;
      }
      long queryCount = _queryCount.getAndSet(0);
      long queryTime = _queryTime.getAndSet(0);
      long queryExceptionCount = _queryExceptionCount.getAndSet(0);

      long dataFetchCount = _dataFetchCount.getAndSet(0);
      long dataFetchTime = _dataFetchTime.getAndSet(0);
      long dataFetchRecordCount = _dataFetchRecordCount.getAndSet(0);

      long dataMutateCount = _dataMutateCount.getAndSet(0);
      long dataMutateTime = _dataMutateTime.getAndSet(0);
      long dataMutateRecordCount = _dataMutateRecordCount.getAndSet(0);

      _queryRate[position] = queryCount;
      _queryResponse[position] = TimeUnit.NANOSECONDS.toMillis(queryTime) / (double) queryCount;

      _queryExceptionRate[position] = queryExceptionCount;

      _fetchRecordRate[position] = dataFetchCount;
      _fetchResponse[position] = TimeUnit.NANOSECONDS.toMillis(dataFetchTime) / (double) dataFetchCount;

      _mutateRecordRate[position] = dataMutateCount;
      _mutateResponse[position] = TimeUnit.NANOSECONDS.toMillis(dataMutateTime) / (double) dataMutateCount;

      timestamp[position] = System.currentTimeMillis();
      position++;
    }
  }

  public void recordQuery(long totalTimeNs) {
    _queryCount.incrementAndGet();
    _queryTime.addAndGet(totalTimeNs);
  }

  public void recordQueryExceptions() {
    _queryExceptionCount.incrementAndGet();
  }

  public void recordDataFetch(long totalTimeNs, long records) {
    _dataFetchCount.incrementAndGet();
    _dataFetchTime.addAndGet(totalTimeNs);
    _dataFetchRecordCount.addAndGet(records);
  }

  public void recordDataMutate(long totalTimeNs, long records) {
    _dataMutateCount.incrementAndGet();
    _dataMutateTime.addAndGet(totalTimeNs);
    _dataMutateRecordCount.addAndGet(records);
  }

  public void writeJson(PrintWriter out) {
    synchronized (this) {
      out.print("{\"labels\":[");
      out.print("{\"name\":\"query\",\"style\":{\"stroke\":\"RoyalBlue\"}},");
      out.print("{\"name\":\"fetch\",\"style\":{\"stroke\":\"Black\"}},");
      out.print("{\"name\":\"mutate\",\"style\":{\"stroke\":\"Yellow\"}},");
      out.print("{\"name\":\"except\",\"style\":{\"stroke\":\"Red\"}}");
      out.print("],\"data\":[");
      int p = position;
      boolean comma = false;
      for (int i = 0; i < sampleSize; i++, p++) {
        if (p >= sampleSize) {
          p = 0;
        }
        double query = _queryRate[p];
        double fetch = _fetchRecordRate[p];
        double mutate = _mutateRecordRate[p];
        double except = _queryExceptionRate[p];
        long t = timestamp[p];
        if (t == 0) {
          continue;
        }
        if (comma) {
          out.print(",");
        }
        out.print("{\"query\":");
        out.print(query);
        out.print(",\"fetch\":");
        out.print(fetch);
        out.print(",\"mutate\":");
        out.print(mutate);
        out.print(",\"except\":");
        out.print(except);
        out.print(",\"recordTime\":");
        out.print(t);
        out.print('}');
        comma = true;
      }
      out.print("]}");
    }
  }
}
