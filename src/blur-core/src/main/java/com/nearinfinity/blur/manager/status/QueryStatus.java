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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.QueryState;

public class QueryStatus implements Comparable<QueryStatus> {

  private final static boolean CPU_TIME_SUPPORTED = ManagementFactory.getThreadMXBean().isCurrentThreadCpuTimeSupported();

  private BlurQuery blurQuery;
  private String table;
  private long startingTime;
  private boolean finished = false;
  private long finishedTime;
  private AtomicLong cpuTimeOfFinishedThreads = new AtomicLong();
  private ThreadMXBean bean = ManagementFactory.getThreadMXBean();
  private long ttl;
  private AtomicInteger totalThreads = new AtomicInteger();
  
  private ThreadLocal<AtomicLong> cpuTimes = new ThreadLocal<AtomicLong>() {
    @Override
    protected AtomicLong initialValue() {
      return new AtomicLong();
    }
  };
  private AtomicLongArray threadsIds;
  private AtomicInteger threadsIdCount = new AtomicInteger(-1);
  private AtomicLongArray completedThreadsIds;
  private AtomicInteger completedThreadsIdCount = new AtomicInteger(-1);
  private boolean interrupted;
  

  public QueryStatus(long ttl, String table, BlurQuery blurQuery, int maxNumberOfThreads) {
    this.ttl = ttl;
    this.table = table;
    this.blurQuery = blurQuery;
    this.startingTime = System.currentTimeMillis();
    this.threadsIds = new AtomicLongArray(maxNumberOfThreads);
    this.completedThreadsIds = setInitalValues(new AtomicLongArray(maxNumberOfThreads));
  }

  private AtomicLongArray setInitalValues(AtomicLongArray atomicLongArray) {
    for (int i = 0; i < atomicLongArray.length(); i++) {
      atomicLongArray.set(i, -1L);
    }
    return atomicLongArray;
  }

  public QueryStatus attachThread() {
    if (CPU_TIME_SUPPORTED) {
      cpuTimes.get().set(bean.getCurrentThreadCpuTime());
    } else {
      cpuTimes.get().set(-1L);
    }
    totalThreads.incrementAndGet();
    Thread currentThread = Thread.currentThread();
    long id = currentThread.getId();
    int index = threadsIdCount.incrementAndGet();
    threadsIds.set(index, id);
    return this;
  }

  public QueryStatus deattachThread() {
    long startingThreadCpuTime = cpuTimes.get().get();
    long currentThreadCpuTime = bean.getCurrentThreadCpuTime();
    cpuTimeOfFinishedThreads.addAndGet(currentThreadCpuTime - startingThreadCpuTime);
    int count = completedThreadsIdCount.incrementAndGet();
    completedThreadsIds.set(count, Thread.currentThread().getId());
    return this;
  }

  public long getUserUuid() {
    return blurQuery.uuid;
  }

  public void cancelQuery() {
    Thread currentThread = Thread.currentThread();
    ThreadGroup group = currentThread.getThreadGroup();
    int count = group.activeCount();
    Thread[] list = new Thread[count];
    
    int num = group.enumerate(list);
    int threadCount = threadsIdCount.get();
    while (!isAllCanceled()) {
      for (int i = 0; i < num; i++) {
        Thread thread = list[i];
        long id = thread.getId();
        INNER:
        for (int j = 0; j < threadCount; j++) {
          long threadId = threadsIds.get(j);
          if (threadId == -1L) {
            continue INNER;
          }
          if (id == threadId && !isAlreadyFinished(id)) {
            //blowup and remove from list;
            thread.interrupt();
            threadsIds.set(j, -1L);
          }
        }
      }
    }
    interrupted = true;
  }

  private boolean isAlreadyFinished(long id) {
    for (int i = 0; i < completedThreadsIds.length(); i++) {
      if (completedThreadsIds.get(i) == id) {
        return true;
      }
    }
    return false;
  }

  private boolean isAllCanceled() {
    for (int i = 0; i < threadsIdCount.get(); i++) {
      long threadId = threadsIds.get(i);
      if (threadId != 1L) {
        return false;
      }
    }
    return true;
  }

  public BlurQueryStatus getQueryStatus() {
    BlurQueryStatus queryStatus = new BlurQueryStatus();
    queryStatus.query = blurQuery;
    queryStatus.totalShards = totalThreads.get();
    queryStatus.completeShards = completedThreadsIdCount.get() + 1;
    queryStatus.state = getQueryState();
    if (queryStatus.query != null) {
      queryStatus.uuid = queryStatus.query.uuid;
    }
    return queryStatus;
  }

  private QueryState getQueryState() {
    if (interrupted) {
      return QueryState.INTERRUPTED;
    } else if (finished) {
      return QueryState.COMPLETE;
    } else {
      return QueryState.RUNNING;
    }
  }

  public String getTable() {
    return table;
  }

  public boolean isFinished() {
    return finished;
  }

  public void setFinished(boolean finished) {
    this.finished = finished;
    finishedTime = System.currentTimeMillis();
  }

  public long getFinishedTime() {
    return finishedTime;
  }

  public boolean isValidForCleanUp() {
    if (!isFinished()) {
      return false;
    }
    if (getFinishedTime() + ttl < System.currentTimeMillis()) {
      return true;
    }
    return false;
  }

  @Override
  public int compareTo(QueryStatus o) {
    long startingTime2 = o.startingTime;
    if (startingTime == startingTime2) {
      int hashCode2 = o.hashCode();
      return hashCode() < hashCode2 ? -1 : 1;
    }
    return startingTime < startingTime2 ? -1 : 1;
  }
}
