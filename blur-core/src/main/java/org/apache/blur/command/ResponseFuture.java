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
package org.apache.blur.command;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.thrift.generated.CommandStatus;

public class ResponseFuture<T> implements Future<T> {

  private final Future<T> _future;
  private final AtomicLong _timeWhenNotRunningObserved = new AtomicLong();
  private final long _tombstone;
  private final Command<?> _commandExecuting;
  private final CommandStatus _originalCommandStatusObject;
  private final AtomicBoolean _running;

  public ResponseFuture(long tombstone, Future<T> future, Command<?> commandExecuting,
      CommandStatus originalCommandStatusObject, AtomicBoolean running) {
    _tombstone = tombstone;
    _future = future;
    _commandExecuting = commandExecuting;
    if (_commandExecuting.getCommandExecutionId() == null) {
      _commandExecuting.setCommandExecutionId(UUID.randomUUID().toString());
    }
    _originalCommandStatusObject = originalCommandStatusObject;
    _running = running;
  }

  public CommandStatus getOriginalCommandStatusObject() {
    return _originalCommandStatusObject;
  }

  public Command<?> getCommandExecuting() {
    return _commandExecuting;
  }

  public boolean cancel(boolean mayInterruptIfRunning) {
    _running.set(false);
    return _future.cancel(mayInterruptIfRunning);
  }

  public boolean isCancelled() {
    return _future.isCancelled();
  }

  public boolean isDone() {
    return _future.isDone();
  }

  public T get() throws InterruptedException, ExecutionException {
    return _future.get();
  }

  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return _future.get(timeout, unit);
  }

  public boolean isRunning() {
    if (isDone() || isCancelled()) {
      _timeWhenNotRunningObserved.compareAndSet(0, System.currentTimeMillis());
      return false;
    }
    return true;
  }

  public boolean hasExpired() {
    long timeWhenNotRunningObserved = _timeWhenNotRunningObserved.get();
    if (timeWhenNotRunningObserved + _tombstone < System.currentTimeMillis()) {
      return true;
    }
    return false;
  }

}
