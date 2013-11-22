/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.trace;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Trace {

  private static final Tracer DO_NOTHING = new Tracer() {
    @Override
    public void done() {

    }
  };
  private static ThreadLocal<TraceCollector> _tracer = new ThreadLocal<TraceCollector>();
  private static TraceReporter _reporter;

  public static void setupTrace(String id) {
    TraceCollector collector = new TraceCollector(id);
    _tracer.set(collector);
  }
  
  private static void setupTraceOnNewThread(TraceCollector parentCollector) {
    TraceCollector collector = new TraceCollector(getNewThreadId(parentCollector));
    _tracer.set(collector);
  }

  private static String getNewThreadId(TraceCollector parentCollector) {
    return parentCollector._id + ":" + Thread.currentThread().getName() + "/" + UUID.randomUUID().toString();
  }

  public static void tearDownTrace() {
    TraceCollector collector = _tracer.get();
    _tracer.set(null);
    if (_reporter != null && collector != null) {
      _reporter.report(collector);
    }
  }

  public static Tracer trace(String name) {
    TraceCollector collector = _tracer.get();
    if (collector == null) {
      return DO_NOTHING;
    }
    TracerImpl tracer = new TracerImpl(name, collector.getNextId());
    collector.add(tracer);
    return tracer;
  }

  public static TraceReporter getReporter() {
    return _reporter;
  }

  public static void setReporter(TraceReporter reporter) {
    _reporter = reporter;
  }

  public static Runnable getRunnable(final Runnable runnable) {
    final TraceCollector tc = _tracer.get();
    if (tc == null) {
      return runnable;
    }
    return new Runnable() {
      @Override
      public void run() {
        setupTraceOnNewThread(tc);
        try {
          runnable.run();
        } finally {
          tearDownTrace();
        }
      }
    };
  }

  public static <V> Callable<V> getCallable(final Callable<V> callable) {
    final TraceCollector tc = _tracer.get();
    if (tc == null) {
      return callable;
    }
    return new Callable<V>() {
      @Override
      public V call() throws Exception {
        setupTraceOnNewThread(tc);
        try {
          return callable.call();
        } finally {
          tearDownTrace();
        }
      }
    };
  }

  public static <V> Collection<Callable<V>> getCallables(final Collection<Callable<V>> callables) {
    TraceCollector tc = _tracer.get();
    if (tc == null) {
      return callables;
    }
    Collection<Callable<V>> list = new ArrayList<Callable<V>>(callables.size());
    for (Callable<V> c : callables) {
      list.add(getCallable(c));
    }
    return list;
  }

  public static String getTraceId() {
    TraceCollector collector = _tracer.get();
    if (collector == null) {
      return null;
    }
    return collector._id;
  }

  public static boolean isTraceRunning() {
    TraceCollector collector = _tracer.get();
    if (collector == null) {
      return false;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  public static ExecutorService getExecutorService(final ExecutorService service) {
    return new ExecutorService() {
      ExecutorService _service = service;

      public void execute(Runnable command) {
        _service.execute(getRunnable(command));
      }

      public <T> Future<T> submit(Callable<T> task) {
        return _service.submit(getCallable(task));
      }

      public <T> Future<T> submit(Runnable task, T result) {
        return _service.submit(getRunnable(task), result);
      }

      public Future<?> submit(Runnable task) {
        return _service.submit(getRunnable(task));
      }

      public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return _service.invokeAll((Collection<? extends Callable<T>>) getCallable((Callable<T>) tasks));
      }

      public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
          throws InterruptedException {
        return _service.invokeAll((Collection<? extends Callable<T>>) getCallable((Callable<T>) tasks), timeout, unit);
      }
      
      public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return _service.invokeAny((Collection<? extends Callable<T>>) getCallable((Callable<T>) tasks));
      }

      public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        return _service.invokeAny((Collection<? extends Callable<T>>) getCallable((Callable<T>) tasks), timeout, unit);
      }

      // pass-through

      public void shutdown() {
        _service.shutdown();
      }

      public List<Runnable> shutdownNow() {
        return _service.shutdownNow();
      }

      public boolean isShutdown() {
        return _service.isShutdown();
      }

      public boolean isTerminated() {
        return _service.isTerminated();
      }

      public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return _service.awaitTermination(timeout, unit);
      }

    };
  }
}
