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
package org.apache.blur.concurrent;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

public class BlurThreadPoolExecutor extends ThreadPoolExecutor {

  private static final Log LOG = LogFactory.getLog(BlurThreadPoolExecutor.class);

  private final List<ThreadBoundaryProcessor> _processorCollection = new ArrayList<ThreadBoundaryProcessor>();

  public BlurThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
      BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
  }

  public void add(ThreadBoundaryProcessor processor) {
    _processorCollection.add(processor);
  }

  public void remove(ThreadBoundaryProcessor processor) {
    _processorCollection.remove(processor);
  }

  public List<ThreadBoundaryProcessor> getPrePostCollection() {
    return _processorCollection;
  }

  @Override
  public void execute(Runnable command) {
    super.execute(wrapRunnable(command));
  }

  @Override
  public Future<?> submit(Runnable task) {
    return super.submit(wrapRunnable(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return super.submit(wrapRunnable(task), result);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return super.submit(wrapCallable(task));
  }

  private Runnable wrapRunnable(final Runnable runnable) {
    final Object[] vars = preprocessCallingThread();
    return new Runnable() {
      @Override
      public void run() {
        Closeable[] closeables = preprocessNewThread(vars);
        try {
          runnable.run();
        } finally {
          cleanup(closeables);
        }
      }
    };
  }

  protected void cleanup(Closeable[] closeables) {
    for (Closeable closeable : closeables) {
      cleanup(closeable);
    }
  }

  private void cleanup(Closeable closeable) {
    try {
      closeable.close();
    } catch (IOException e) {
      LOG.error("Error during quiet close of [" + closeable + "] [" + e.getMessage() + "]");
    }
  }

  private Closeable[] preprocessNewThread(Object[] vars) {
    int size = _processorCollection.size();
    Closeable[] closeables = new Closeable[size];
    for (int i = 0; i < size; i++) {
      closeables[i] = _processorCollection.get(i).preprocessNewThread(vars[i]);
    }
    return closeables;
  }

  private Object[] preprocessCallingThread() {
    int size = _processorCollection.size();
    Object[] vars = new Object[size];
    for (int i = 0; i < size; i++) {
      vars[i] = _processorCollection.get(i).preprocessCallingThread();
    }
    return vars;
  }

  private <T> Callable<T> wrapCallable(final Callable<T> callable) {
    final Object[] vars = preprocessCallingThread();
    return new Callable<T>() {
      @Override
      public T call() throws Exception {
        Closeable[] closeables = preprocessNewThread(vars);
        try {
          return callable.call();
        } finally {
          cleanup(closeables);
        }
      }
    };
  }
}
