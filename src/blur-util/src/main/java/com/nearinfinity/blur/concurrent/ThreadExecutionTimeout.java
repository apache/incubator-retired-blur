package com.nearinfinity.blur.concurrent;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

public class ThreadExecutionTimeout {
  
  private Timer _timer;
  private long _delay = TimeUnit.SECONDS.toMillis(1);
  private ConcurrentSkipListSet<ThreadTimeout> _threads = new ConcurrentSkipListSet<ThreadTimeout>();
  
  private static class ThreadTimeout implements Comparable<ThreadTimeout> {
    public ThreadTimeout(long timeToInterrupt, Thread thread) {
      _thread = thread;
      _timeToInterrupt = timeToInterrupt;
    }
    Thread _thread;
    long _timeToInterrupt;
    
    @Override
    public int compareTo(ThreadTimeout o) {
      if (_timeToInterrupt < o._timeToInterrupt) {
        return -1;
      }
      if (_timeToInterrupt == o._timeToInterrupt) {
        return _thread.getName().compareTo(o._thread.getName());
      }
      return 1;
    }

    @Override
    public String toString() {
      return "ThreadTimeout [_thread=" + _thread + ", _timeToInterrupt=" + _timeToInterrupt + "]";
    }
  }

  public static void main(String[] args) {
    final ThreadExecutionTimeout t = new ThreadExecutionTimeout();
    t.init();
    new Thread(new Runnable() {
      @Override
      public void run() {
        t.timeout(TimeUnit.SECONDS.toMillis(3));
        System.out.println("Running");
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        }
        System.out.println("DONE");
      }
    }).start();
  }

  public void init() {
    _timer = new Timer("ThreadExecutionTimeout-Daemon", true);
    _timer.schedule(new TimerTask() {
      @Override
      public void run() {
        tryToInterrupt();
      }
    }, _delay, _delay);
  }
  
  public void close() {
    _timer.cancel();
    _timer.purge();
  }

  private void tryToInterrupt() {
    for (ThreadTimeout threadTimeout : _threads) {
      if (threadTimeout._timeToInterrupt < System.currentTimeMillis()) {
        interrupt(threadTimeout);
      }
    }
  }

  private void interrupt(ThreadTimeout threadTimeout) {
    Thread thread = threadTimeout._thread;
    if (thread.isAlive()) {
      thread.interrupt();
    } else {
      _threads.remove(threadTimeout);
    }
  }

  public void timeout(long timeout) {
    Thread thread = Thread.currentThread();
    timeout(timeout,thread);
  }

  public void timeout(long timeout, Thread thread) {
    ThreadTimeout threadTimeout = new ThreadTimeout(timeout + System.currentTimeMillis(),thread);
    _threads.add(threadTimeout);
  }

  public void setDelay(long delay) {
    _delay = delay;
  }

}
