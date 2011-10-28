package com.nearinfinity.blur.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class ThreadWatcher {
  
  private static final Log LOG = LogFactory.getLog(ThreadWatcher.class);

  public static void main(String[] args) {

  }
  
  static class Watch {
    public Watch(Thread thread) {
      _thread = thread;
    }
    Thread _thread;
    final long _start = System.currentTimeMillis();
  }
  
  private ConcurrentMap<Thread,Watch> _threads = new ConcurrentHashMap<Thread, Watch>();
  private Timer _timer;
  
  public void watch(Thread thread) {
    _threads.put(thread, new Watch(thread));
  }
  
  public void release(Thread thread) {
    _threads.remove(thread);
  }

  public ExecutorService watch(ExecutorService executorService) {
    return new ThreadWatcherExecutorService(executorService,this);
  }

  public void init() {
    _timer = new Timer("Thread-Watcher",true);
    _timer.schedule(new TimerTask() {
      @Override
      public void run() {
        processRunningThreads();
      }
    }, TimeUnit.SECONDS.toMillis(5), TimeUnit.SECONDS.toMillis(5));
  }
  
  private void processRunningThreads() {
    for (Entry<Thread,Watch> entry : _threads.entrySet()) {
      processWatch(entry.getValue());
    }
  }

  private void processWatch(Watch watch) {
    if (hasBeenExecutingLongerThan(TimeUnit.SECONDS.toMillis(5),watch)) {
      long now = System.currentTimeMillis();
      LOG.info("Thread [{0}] has been executing for [{1} ms]",watch._thread,now - watch._start);
    }
  }

  private boolean hasBeenExecutingLongerThan(long period, Watch watch) {
    if (watch._start + period < System.currentTimeMillis()) {
      return true;
    }
    return false;
  }

  public void close() {
    _timer.cancel();
    _timer.purge();
    _threads.clear();
  }
  
  public static class ThreadWatcherExecutorService implements ExecutorService {
    
    private ExecutorService _executorService;
    private ThreadWatcher _threadWatcher;
    
    public ThreadWatcherExecutorService(ExecutorService executorService, ThreadWatcher threadWatcher) {
      _executorService = executorService;
      _threadWatcher = threadWatcher;
    }

    private Runnable wrap(final Runnable runnable) {
      return new Runnable() {
        @Override
        public void run() {
          Thread thread = Thread.currentThread();
          _threadWatcher.watch(thread);
          try {
            runnable.run();
          } finally {
            _threadWatcher.release(thread);
          }
        }
      }; 
    }
    
    private <T> Collection<? extends Callable<T>> wrapCallableCollection(Collection<? extends Callable<T>> tasks) {
      List<Callable<T>> result = new ArrayList<Callable<T>>(tasks.size());
      for (Callable<T> callable : tasks) {
        result.add(wrapCallable(callable));
      }
      return result;
    }
    
    private <T> Callable<T> wrapCallable(final Callable<T> task) {
      return new Callable<T>() {
        @Override
        public T call() throws Exception {
          Thread thread = Thread.currentThread();
          _threadWatcher.watch(thread);
          try {
            return task.call();
          } finally {
            _threadWatcher.release(thread);
          }
        }
      };
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return _executorService.awaitTermination(timeout, unit);
    }

    public void execute(Runnable command) {
      _executorService.execute(wrap(command));
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
      return _executorService.invokeAll(wrapCallableCollection(tasks), timeout, unit);
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
      return _executorService.invokeAll(wrapCallableCollection(tasks));
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return _executorService.invokeAny(wrapCallableCollection(tasks), timeout, unit);
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
      return _executorService.invokeAny(wrapCallableCollection(tasks));
    }

    public boolean isShutdown() {
      return _executorService.isShutdown();
    }

    public boolean isTerminated() {
      return _executorService.isTerminated();
    }

    public void shutdown() {
      _executorService.shutdown();
    }

    public List<Runnable> shutdownNow() {
      return _executorService.shutdownNow();
    }

    public <T> Future<T> submit(Callable<T> task) {
      return _executorService.submit(wrapCallable(task));
    }

    public <T> Future<T> submit(Runnable task, T result) {
      return _executorService.submit(wrap(task), result);
    }

    public Future<?> submit(Runnable task) {
      return _executorService.submit(wrap(task));
    }

    
  }

}
