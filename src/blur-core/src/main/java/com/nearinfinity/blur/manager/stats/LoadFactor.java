package com.nearinfinity.blur.manager.stats;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class LoadFactor {

  private static final Log LOG = LogFactory.getLog(LoadFactor.class);

  public static void main(String[] args) throws InterruptedException {
    LoadFactor loadFactor = new LoadFactor();
    loadFactor.init();
    loadFactor.add("heapUsed", new Sampler() {
      private MemoryMXBean bean = ManagementFactory.getMemoryMXBean();

      @Override
      public long sample() {
        return bean.getHeapMemoryUsage().getUsed();
      }
    });

    new Thread(new Runnable() {
      @Override
      public void run() {
        long total = 0;
        while (true) {
          total += doWork();
        }
      }
    }).start();

    while (true) {
      System.out.println("one     = " + (long) loadFactor.getOneMinuteLoadFactor("heapUsed"));
      System.out.println("five    = " + (long) loadFactor.getFiveMinuteLoadFactor("heapUsed"));
      System.out.println("fifteen = " + (long) loadFactor.getFifteenMinuteLoadFactor("heapUsed"));
      Thread.sleep(5000);
    }

  }

  protected static int doWork() {
    StringBuilder builder = new StringBuilder();
    int count = 0;
    for (int i = 0; i < 10000000; i++) {
      if (count == 1000) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          return 0;
        }
        count = 0;
      }
      builder.append('m');
      count++;
    }
    return builder.toString().hashCode();
  }

  private Map<String, LoadFactorProcessor> _processors = new ConcurrentHashMap<String, LoadFactorProcessor>();
  private Timer _timer;
  private long _delay = TimeUnit.SECONDS.toMillis(1);
  private long _period = TimeUnit.SECONDS.toMillis(1);

  public void init() {
    _timer = new Timer("LoadFactor-Daemon", true);
    _timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          sampleAll();
        } catch (Throwable e) {
          LOG.error("Unknown error", e);
        }
      }
    }, _delay, _period);

  }

  private void sampleAll() {
    for (String name : _processors.keySet()) {
      LoadFactorProcessor processor = _processors.get(name);
      processor.sample();
    }
  }

  public void add(String name, Sampler sampler) {
    _processors.put(name, new LoadFactorProcessor(sampler));
  }

  public double getOneMinuteLoadFactor(String name) {
    LoadFactorProcessor processor = _processors.get(name);
    if (processor == null) {
      return 0;
    }
    return processor.oneMinuteLoadFactor();
  }

  public double getFiveMinuteLoadFactor(String name) {
    LoadFactorProcessor processor = _processors.get(name);
    if (processor == null) {
      return 0;
    }
    return processor.fiveMinuteLoadFactor();
  }

  public double getFifteenMinuteLoadFactor(String name) {
    LoadFactorProcessor processor = _processors.get(name);
    if (processor == null) {
      return 0;
    }
    return processor.fifteenMinuteLoadFactor();
  }

}
