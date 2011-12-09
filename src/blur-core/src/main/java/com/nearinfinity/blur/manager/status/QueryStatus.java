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

package com.nearinfinity.blur.manager.status;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.QueryState;

public class QueryStatus implements Comparable<QueryStatus> {

  private final static boolean CPU_TIME_SUPPORTED = ManagementFactory.getThreadMXBean().isCurrentThreadCpuTimeSupported();

  private final BlurQuery _blurQuery;
  private final String _table;
  private final long _startingTime;
  private boolean _finished = false;
  private long _finishedTime;
  private final AtomicLong _cpuTimeOfFinishedThreads = new AtomicLong();
  private final ThreadMXBean _bean = ManagementFactory.getThreadMXBean();
  private final long _ttl;
  private final ThreadLocal<Long> _cpuTimes = new ThreadLocal<Long>();
  private final AtomicBoolean _interrupted = new AtomicBoolean(false);
  private final AtomicInteger _totalShards = new AtomicInteger();
  private final AtomicInteger _completeShards = new AtomicInteger();
  private final List<Thread> _threads = Collections.synchronizedList(new ArrayList<Thread>());

  public QueryStatus(long ttl, String table, BlurQuery blurQuery) {
    _ttl = ttl;
    _table = table;
    _blurQuery = blurQuery;
    _startingTime = System.currentTimeMillis();
  }

  public QueryStatus attachThread() {
    if (_interrupted.get()) {
      Thread.currentThread().interrupt();
    }
    if (CPU_TIME_SUPPORTED) {
      _cpuTimes.set(_bean.getCurrentThreadCpuTime());
    } else {
      _cpuTimes.set(-1L);
    }
    _totalShards.incrementAndGet();
    _threads.add(Thread.currentThread());
    return this;
  }

  public QueryStatus deattachThread() {
    if (_interrupted.get()) {
      Thread.currentThread().interrupt();
    }
    _completeShards.incrementAndGet();
    if (CPU_TIME_SUPPORTED) {
      long startingThreadCpuTime = _cpuTimes.get();
      long currentThreadCpuTime = _bean.getCurrentThreadCpuTime();
      _cpuTimeOfFinishedThreads.addAndGet(currentThreadCpuTime - startingThreadCpuTime);
    }
    return this;
  }

  public long getUserUuid() {
    return _blurQuery.uuid;
  }

  public void cancelQuery() {
    _interrupted.set(true);
    for (Thread t : _threads) {
      t.interrupt();
    }
  }

  public BlurQueryStatus getQueryStatus() {
    BlurQueryStatus queryStatus = new BlurQueryStatus();
    queryStatus.query = _blurQuery;
    queryStatus.totalShards = _totalShards.get();
    queryStatus.completeShards = _completeShards.get();
    queryStatus.state = getQueryState();
    if (queryStatus.query != null) {
      queryStatus.uuid = queryStatus.query.uuid;
    }
    return queryStatus;
  }

  private QueryState getQueryState() {
    if (_interrupted.get()) {
      return QueryState.INTERRUPTED;
    } else if (_finished) {
      return QueryState.COMPLETE;
    } else {
      return QueryState.RUNNING;
    }
  }

  public String getTable() {
    return _table;
  }

  public boolean isFinished() {
    return _finished;
  }

  public void setFinished(boolean finished) {
    this._finished = finished;
    _finishedTime = System.currentTimeMillis();
  }

  public long getFinishedTime() {
    return _finishedTime;
  }

  public boolean isValidForCleanUp() {
    if (!isFinished()) {
      return false;
    }
    if (getFinishedTime() + _ttl < System.currentTimeMillis()) {
      return true;
    }
    return false;
  }

  @Override
  public int compareTo(QueryStatus o) {
    long startingTime2 = o._startingTime;
    if (_startingTime == startingTime2) {
      int hashCode2 = o.hashCode();
      return hashCode() < hashCode2 ? -1 : 1;
    }
    return _startingTime < startingTime2 ? -1 : 1;
  }
}
