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

import java.util.concurrent.Callable;

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
    TracerImpl tracer = new TracerImpl(name, collector.getNextId(), collector.getParentThreadId());
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
    TraceCollector tc = _tracer.get();
    if (tc == null) {
      return runnable;
    }
    final TraceCollector traceCollector = new TraceCollector(tc);
    return new Runnable() {
      @Override
      public void run() {
        _tracer.set(traceCollector);
        try {
          runnable.run();
        } finally {
          _tracer.set(null);
        }
      }
    };
  }

  public static <V> Callable<V> getRunnable(final Callable<V> callable) {
    TraceCollector tc = _tracer.get();
    if (tc == null) {
      return callable;
    }
    final TraceCollector traceCollector = new TraceCollector(tc);
    return new Callable<V>() {
      @Override
      public V call() throws Exception {
        _tracer.set(traceCollector);
        try {
          return callable.call();
        } finally {
          _tracer.set(null);
        }
      }
    };
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

}
