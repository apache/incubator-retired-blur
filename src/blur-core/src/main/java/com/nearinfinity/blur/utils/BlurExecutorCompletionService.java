/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BlurExecutorCompletionService<T> extends ExecutorCompletionService<T> {

  private AtomicInteger count = new AtomicInteger(0);
  private Collection<Future<T>> bag;

  public BlurExecutorCompletionService(Executor executor) {
    super(executor);
    bag = Collections.synchronizedCollection(new HashSet<Future<T>>());
  }
  
  public void cancelAll() {
    for (Future<T> future : bag) {
      future.cancel(true);
    }
  }
  
  private Future<T> remember(Future<T> future) {
    bag.add(future);
    return future;
  }
  
  private Future<T> forget(Future<T> future) {
    bag.remove(future);
    return future;
  }

  public int getRemainingCount() {
    return count.get();
  }

  @Override
  public Future<T> poll() {
    Future<T> poll = super.poll();
    if (poll != null) {
      count.decrementAndGet();
    }
    return forget(poll);
  }

  @Override
  public Future<T> poll(long timeout, TimeUnit unit) throws InterruptedException {
    Future<T> poll = super.poll(timeout, unit);
    if (poll != null) {
      count.decrementAndGet();
    }
    return forget(poll);
  }
  
  @Override
  public Future<T> submit(Callable<T> task) {
    Future<T> future = super.submit(task);
    count.incrementAndGet();
    return remember(future);
  }
  
  @Override
  public Future<T> submit(Runnable task, T result) {
    Future<T> future = super.submit(task, result);
    count.incrementAndGet();
    return remember(future);
  }

  @Override
  public Future<T> take() throws InterruptedException {
    Future<T> take = super.take();
    if (take != null) {
      count.decrementAndGet();
    }
    return forget(take);
  }

}
