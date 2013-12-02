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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Trace {

  private static final String REQUEST_ID = "requestId";

  public static class TraceId {
    final String _rootId;
    final String _requestId;

    public TraceId(String rootId, String requestId) {
      _rootId = rootId;
      _requestId = requestId;
    }

    public String getRootId() {
      return _rootId;
    }

    public String getRequestId() {
      return _requestId;
    }

    public String toJson() {
      return "{\"rootId\":\"" + _rootId + "\",\"requestId\":\"" + _requestId + "\"}";
    }
  }

  public static class Parameter {

    final String _name;
    final String _value;

    public Parameter(String name, String value) {
      _name = name;
      _value = value;
    }

  }

  private static final Tracer DO_NOTHING = new Tracer() {
    @Override
    public void done() {

    }
  };
  private static ThreadLocal<TraceCollector> _tracer = new ThreadLocal<TraceCollector>();
  private static TraceStorage _storage;
  private static String _nodeName;
  private static ThreadLocal<Random> _random = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  public static String getNodeName() {
    return _nodeName;
  }

  public static void setNodeName(String nodeName) {
    _nodeName = nodeName;
  }

  public static void setupTrace(String rootId) {
    setupTrace(rootId, rootId);
  }

  public static void setupTrace(String rootId, String requestId) {
    TraceId traceId = new TraceId(rootId, requestId);
    TraceCollector collector = new TraceCollector(_nodeName, traceId);
    _tracer.set(collector);
  }

  public static Parameter param(Object name, Object value) {
    if (name == null) {
      name = "null";
    }
    if (value == null) {
      value = "null";
    }
    return new Parameter(name.toString(), value.toString());
  }

  private static void setupTraceOnNewThread(TraceCollector parentCollector, String requestId, int traceScope) {
    TraceCollector traceCollector = new TraceCollector(parentCollector, requestId);
    TracerImpl tracer = new TracerImpl(traceCollector, parentCollector.getNextId(), traceScope, requestId);
    parentCollector.add(tracer);
    _tracer.set(traceCollector);
  }

  private static void tearDownTraceOnNewThread() {
    _tracer.set(null);
  }

  public static void tearDownTrace() {
    TraceCollector collector = _tracer.get();
    _tracer.set(null);
    if (_storage != null && collector != null) {
      _storage.store(collector);
    }
  }

  public static Tracer trace(String desc, Parameter... parameters) {
    TraceCollector collector = _tracer.get();
    if (collector == null) {
      return DO_NOTHING;
    }
    TracerImpl tracer = new TracerImpl(desc, parameters, collector.getNextId(), collector.getScope());
    collector.add(tracer);
    return tracer;
  }

  public static TraceStorage getStorage() {
    return _storage;
  }

  public static void setStorage(TraceStorage storage) {
    _storage = storage;
  }

  public static Runnable getRunnable(final Runnable runnable) {
    final TraceCollector tc = _tracer.get();
    if (tc == null) {
      return runnable;
    }
    final long requestId = _random.get().nextLong();
    Tracer trace = Trace.trace("new runnable", Trace.param(REQUEST_ID, requestId));
    TracerImpl impl = (TracerImpl) trace;
    final int traceScope = impl._traceScope;
    try {
      return new Runnable() {
        @Override
        public void run() {
          setupTraceOnNewThread(tc, Long.toString(requestId), traceScope);
          Tracer t = Trace.trace("executing runnable", Trace.param(REQUEST_ID, requestId));
          try {
            runnable.run();
          } finally {
            t.done();
            tearDownTraceOnNewThread();
          }
        }
      };
    } finally {
      trace.done();
    }
  }

  public static <V> Callable<V> getCallable(final Callable<V> callable) {
    final TraceCollector tc = _tracer.get();
    if (tc == null) {
      return callable;
    }
    final long requestId = _random.get().nextLong();
    Tracer trace = Trace.trace("new callable", Trace.param(REQUEST_ID, requestId));
    TracerImpl impl = (TracerImpl) trace;
    final int traceScope = impl._traceScope;
    try {
      return new Callable<V>() {
        @Override
        public V call() throws Exception {
          setupTraceOnNewThread(tc, Long.toString(requestId), traceScope);
          Tracer t = Trace.trace("executing callable", Trace.param(REQUEST_ID, requestId));
          try {
            return callable.call();
          } finally {
            t.done();
            tearDownTraceOnNewThread();
          }
        }
      };
    } finally {
      trace.done();
    }
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

  public static TraceId getTraceId() {
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
