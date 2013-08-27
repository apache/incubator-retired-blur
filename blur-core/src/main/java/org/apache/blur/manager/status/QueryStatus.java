package org.apache.blur.manager.status;

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
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.CpuTime;
import org.apache.blur.thrift.generated.QueryState;

/**
 * This class is accessed by multiple threads (one for each shard) executing the
 * query. Tracks status and collects metrics
 * 
 */
public class QueryStatus implements Comparable<QueryStatus> {

  private final static boolean CPU_TIME_SUPPORTED = ManagementFactory.getThreadMXBean()
      .isCurrentThreadCpuTimeSupported();

  private final BlurQuery _blurQuery;
  private final String _table;
  private final long _startingTime;
  private boolean _finished = false;
  private long _finishedTime;
  private final ThreadMXBean _bean = ManagementFactory.getThreadMXBean();
  private final long _ttl;
  private final AtomicReference<QueryState> _state = new AtomicReference<QueryState>();
  private final AtomicInteger _totalShards = new AtomicInteger();
  private final AtomicInteger _completeShards = new AtomicInteger();
  private final AtomicBoolean _running;
  private final Map<String, CpuTime> _cpuTimes = new ConcurrentHashMap<String, CpuTime>();

  public QueryStatus(long ttl, String table, BlurQuery blurQuery, AtomicBoolean running) {
    _ttl = ttl;
    _table = table;
    _blurQuery = blurQuery;
    _startingTime = System.currentTimeMillis();
    _running = running;
    _state.set(QueryState.RUNNING);
  }

  public QueryStatus attachThread(String shardName) {
    CpuTime cpuTime = new CpuTime();
    if (CPU_TIME_SUPPORTED) {
      cpuTime.cpuTime = _bean.getCurrentThreadCpuTime();
    } else {
      cpuTime.cpuTime = -1L;
    }
    cpuTime.realTime = System.nanoTime();
    _cpuTimes.put(shardName, cpuTime);
    _totalShards.incrementAndGet();
    return this;
  }

  public QueryStatus deattachThread(String shardName) {
    _completeShards.incrementAndGet();
    CpuTime cpuTime = _cpuTimes.get(shardName);
    if (CPU_TIME_SUPPORTED) {
      cpuTime.cpuTime = _bean.getCurrentThreadCpuTime() - cpuTime.cpuTime;
    }
    cpuTime.realTime = System.nanoTime() - cpuTime.realTime;
    return this;
  }

  public String getUserUuid() {
    return _blurQuery.uuid;
  }

  public void stopQueryForBackPressure() {
    _state.set(QueryState.BACK_PRESSURE_INTERRUPTED);
    _running.set(false);
  }

  public void cancelQuery() {
    _state.set(QueryState.INTERRUPTED);
    _running.set(false);
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
    queryStatus.cpuTimes = _cpuTimes;
    return queryStatus;
  }

  private QueryState getQueryState() {
    return _state.get();
  }

  public String getTable() {
    return _table;
  }

  public boolean isFinished() {
    return _finished;
  }

  public void setFinished(boolean finished) {
    if (_state.get() == QueryState.RUNNING) {
      _state.set(QueryState.COMPLETE);
    }
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
