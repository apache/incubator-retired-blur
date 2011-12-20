package com.nearinfinity.blur.concurrent;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class ThreadExecutionTimeout {

  private static final Log LOG = LogFactory.getLog(ThreadExecutionTimeout.class);
  private Timer _timer;
  private long _delay = TimeUnit.SECONDS.toMillis(1);
  private ConcurrentMap<Thread, ThreadTimeout> _threads = new ConcurrentHashMap<Thread, ThreadTimeout>();

  private static class ThreadTimeout implements Comparable<ThreadTimeout> {
    public ThreadTimeout(long timeToInterrupt, Thread thread) {
      _thread = thread;
      _timeToInterrupt = timeToInterrupt;
    }

    Thread _thread;
    long _timeToInterrupt;
    AtomicBoolean finished = new AtomicBoolean(false);

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

    public void finished() {
      finished.set(true);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    final ThreadExecutionTimeout t = new ThreadExecutionTimeout();
    t.init();
    new Thread(new Runnable() {
      @Override
      public void run() {
        t.timeout(TimeUnit.SECONDS.toMillis(3));
        System.out.println("Running");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        }
        System.out.println("DONE");
        t.finished();
      }
    }).start();

    Thread.sleep(10000);
  }

  public void init() {
    _timer = new Timer("ThreadExecutionTimeout-Daemon", true);
    _timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          tryToInterrupt();
        } catch (Throwable e) {
          LOG.error("Unknown error",e);
        }
      }
    }, _delay, _delay);
  }

  public void close() {
    _timer.cancel();
    _timer.purge();
  }

  private void tryToInterrupt() {
    for (ThreadTimeout threadTimeout : _threads.values()) {
      if (threadTimeout._timeToInterrupt < System.currentTimeMillis()) {
        interrupt(threadTimeout);
      }
    }
  }

  private void interrupt(ThreadTimeout threadTimeout) {
    Thread thread = threadTimeout._thread;
    if (thread.isAlive() && !threadTimeout.finished.get()) {
      LOG.info("Interrupting thread [" + thread.getName() + "] had timeout of [" + threadTimeout._timeToInterrupt + "]");
      thread.interrupt();
    } else {
      _threads.remove(threadTimeout._thread);
    }
  }

  public void timeout(long timeout) {
    Thread thread = Thread.currentThread();
    timeout(timeout, thread);
  }

  public void finished(Thread thread) {
    ThreadTimeout threadTimeout = _threads.get(thread);
    threadTimeout.finished();
  }

  public void finished() {
    finished(Thread.currentThread());
  }

  public void timeout(long timeout, Thread thread) {
    if (timeout < 0) {
      throw new RuntimeException("Timeout cannot be less than zero.");
    }
    long timeToInterrupt = timeout + System.currentTimeMillis();
    if (timeToInterrupt < 0) {
      timeToInterrupt = Long.MAX_VALUE;
    }
    ThreadTimeout threadTimeout = new ThreadTimeout(timeToInterrupt, thread);
    _threads.put(thread, threadTimeout);
  }

  public void setDelay(long delay) {
    _delay = delay;
  }

}
