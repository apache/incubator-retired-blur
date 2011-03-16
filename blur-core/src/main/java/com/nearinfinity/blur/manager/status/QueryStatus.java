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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;

public class QueryStatus implements Comparable<QueryStatus> {

    private final static boolean CPU_TIME_SUPPORTED = ManagementFactory.getThreadMXBean().isCurrentThreadCpuTimeSupported();
    
    private BlurQuery blurQuery;
    private String table;
    private Map<Thread,Long> threads = new ConcurrentHashMap<Thread,Long>();
    private int totalThreads = 0;
    private long startingTime;
    private boolean finished = false;
    private long finishedTime;
    private AtomicLong cpuTimeOfFinishedThreads = new AtomicLong();
    private ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    private long ttl;

    private boolean interrupted;

    public QueryStatus(long ttl, String table, BlurQuery blurQuery) {
        this.ttl = ttl;
        this.table = table;
        this.blurQuery = blurQuery;
        this.startingTime = System.currentTimeMillis();
    }

    public QueryStatus attachThread() {
        if (CPU_TIME_SUPPORTED) {
            threads.put(Thread.currentThread(), ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime());
        } else {
            threads.put(Thread.currentThread(), -1L);
        }
        totalThreads++;
        return this;
    }

    public QueryStatus deattachThread() {
        Thread thread = Thread.currentThread();
        long startingThreadCpuTime = threads.remove(thread);
        long currentThreadCpuTime = bean.getThreadCpuTime(thread.getId());
        cpuTimeOfFinishedThreads.addAndGet(currentThreadCpuTime - startingThreadCpuTime);
        return this;
    }

    public long getUserUuid() {
        return blurQuery.uuid;
    }

    public void cancelSearch() {
        interrupted = true;
        for (Thread t : threads.keySet()) {
            t.interrupt();
        }
    }

    public BlurQueryStatus getQueryStatus() {
        BlurQueryStatus queryStatus = new BlurQueryStatus();
        queryStatus.query = blurQuery;
        queryStatus.complete = getCompleteStatus();
        if (CPU_TIME_SUPPORTED) {
            queryStatus.cpuTime = getCpuTime();
        }
        queryStatus.running = !finished;
        queryStatus.interrupted = interrupted;
        if (queryStatus.running) {
            queryStatus.realTime = System.currentTimeMillis() - startingTime;
        } else {
            queryStatus.realTime = finishedTime - startingTime;
        }
        if (queryStatus.query != null) {
            queryStatus.uuid = queryStatus.query.uuid;
        }
        return queryStatus;
    }

    private long getCpuTime() {
        long cpuTime = 0;
        for (Entry<Thread,Long> threadEntry : threads.entrySet()) {
            long startingThreadCpuTime = threadEntry.getValue();
            long currentThreadCpuTime = bean.getThreadCpuTime(threadEntry.getKey().getId());
            cpuTime += (currentThreadCpuTime - startingThreadCpuTime);
        }
        return (cpuTime + cpuTimeOfFinishedThreads.get()) / 1000000;//convert to ms from ns
    }

    private double getCompleteStatus() {
        int size = threads.size();
        if (size == 0) {
            return 1.0d;
        }
        return (totalThreads - size) / size;
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
