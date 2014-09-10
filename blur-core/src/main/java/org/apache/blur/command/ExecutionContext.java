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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

import com.google.common.collect.MapMaker;

public class ExecutionContext {

  private static final Log LOG = LogFactory.getLog(ExecutionContext.class);

  private static ConcurrentMap<ExecutionId, ExecutionContext> _contextMap = new MapMaker().makeMap();
  private static ThreadLocal<ExecutionId> _currentId = new ThreadLocal<ExecutionId>();

  public static ExecutionContext create() {
    ExecutionId executionId = new ExecutionId(UUID.randomUUID().toString());
    ExecutionContext executionContext = new ExecutionContext(executionId);
    _contextMap.put(executionId, executionContext);
    _currentId.set(executionId);
    return executionContext;
  }

  public static ExecutionContext get() {
    ExecutionId executionId = _currentId.get();
    return _contextMap.get(executionId);
  }

  private final ExecutionId _executionId;
  private final AtomicBoolean _running;
  private final List<Future<?>> _futures = new ArrayList<Future<?>>();
  private Future<?> _driverFuture;

  public ExecutionContext(ExecutionId executionId) {
    _executionId = executionId;
    _running = new AtomicBoolean(true);
  }

  public void registerFuture(Future<?> future) {
    synchronized (_futures) {
      _futures.add(future);
    }
  }

  public <T> Callable<T> wrapCallable(final Callable<T> callable) {
    return new Callable<T>() {
      @Override
      public T call() throws Exception {
        _currentId.set(_executionId);
        LOG.info("Executing in new thread [{0}]", _executionId.getId());
        try {
          return callable.call();
        } finally {
          _currentId.set(null);
        }
      }
    };
  }

  public ExecutionId getExecutionId() {
    return _executionId;
  }

  public AtomicBoolean getRunning() {
    return _running;
  }

  public <T> void registerDriverFuture(Future<T> driverFuture) {
    _driverFuture = driverFuture;
  }

}
