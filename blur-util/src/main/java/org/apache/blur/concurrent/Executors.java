package org.apache.blur.concurrent;

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.blur.trace.Trace;
import org.apache.blur.user.UserThreadBoundaryProcessor;

public class Executors {

  public static ExecutorService newThreadPool(String prefix, int threadCount) {
    return newThreadPool(prefix, threadCount, true);
  }

  public static ExecutorService newThreadPool(String prefix, int threadCount, boolean watch) {
    return newThreadPool(new LinkedBlockingQueue<Runnable>(), prefix, threadCount, watch);
  }

  public static ExecutorService newThreadPool(BlockingQueue<Runnable> workQueue, String prefix, int threadCount) {
    return newThreadPool(workQueue, prefix, threadCount, true);
  }

  public static ExecutorService newThreadPool(BlockingQueue<Runnable> workQueue, String prefix, int threadCount,
      boolean watch) {
    BlurThreadPoolExecutor executorService = new BlurThreadPoolExecutor(threadCount, threadCount, 10L,
        TimeUnit.SECONDS, workQueue, new BlurThreadFactory(prefix));
    executorService.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    executorService.add(new UserThreadBoundaryProcessor());
    if (watch) {
      return Trace.getExecutorService(ThreadWatcher.instance().watch(executorService));
    }
    return Trace.getExecutorService(executorService);
  }

  public static ExecutorService newSingleThreadExecutor(String prefix) {
    return Trace.getExecutorService(java.util.concurrent.Executors
        .newSingleThreadExecutor(new BlurThreadFactory(prefix)));
  }

  public static class BlurThreadFactory implements ThreadFactory {
    private AtomicInteger threadNumber = new AtomicInteger(0);
    private String prefix;

    public BlurThreadFactory(String prefix) {
      this.prefix = prefix;
    }

    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setName(prefix + threadNumber.getAndIncrement());
      if (t.isDaemon()) {
        t.setDaemon(false);
      }
      return t;
    }
  }

  private Executors() {

  }
}
