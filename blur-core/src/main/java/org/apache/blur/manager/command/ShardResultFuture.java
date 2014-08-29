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
package org.apache.blur.manager.command;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ShardResultFuture<T> implements Future<T> {

  private final Shard _shard;
  private final Future<Map<Shard, T>> _future;

  public boolean cancel(boolean mayInterruptIfRunning) {
    return _future.cancel(mayInterruptIfRunning);
  }

  public boolean isCancelled() {
    return _future.isCancelled();
  }

  public boolean isDone() {
    return _future.isDone();
  }

  public T get() throws InterruptedException, ExecutionException {
    Map<Shard, T> map = _future.get();
    if (map == null) {
      return null;
    }
    return map.get(_shard);
  }

  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
      TimeoutException {
    Map<Shard, T> map = _future.get(timeout, unit);
    if (map == null) {
      return null;
    }
    return map.get(_shard);
  }

  public ShardResultFuture(Shard shard, Future<Map<Shard, T>> future) {
    _shard = shard;
    _future = future;
  }

}
