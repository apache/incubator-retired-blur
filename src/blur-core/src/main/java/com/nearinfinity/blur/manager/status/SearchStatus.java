package com.nearinfinity.blur.manager.status;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.nearinfinity.blur.thrift.generated.Facet;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;

public class SearchStatus {

    private final static boolean CPU_TIME_SUPPORTED = ManagementFactory.getThreadMXBean().isCurrentThreadCpuTimeSupported();
    
    private SearchQuery searchQuery;
    private Facet facet;
    private String table;
    private Map<Thread,Long> threads = new ConcurrentHashMap<Thread,Long>();
    private int totalThreads = 0;
    private long startingTime;
    private boolean finished = false;
    private long finishedTime;
    private AtomicLong cpuTimeOfFinishedThreads = new AtomicLong();
    private ThreadMXBean bean = ManagementFactory.getThreadMXBean();

    private boolean interrupted;

    public SearchStatus(String table, SearchQuery searchQuery) {
        this.table = table;
        this.searchQuery = searchQuery;
        this.startingTime = System.currentTimeMillis();
    }

    public SearchStatus(String table, SearchQuery searchQuery, Facet facet) {
        this.table = table;
        this.searchQuery = searchQuery;
        this.startingTime = System.currentTimeMillis();
        this.facet = facet;
    }

    public SearchStatus attachThread() {
        if (CPU_TIME_SUPPORTED) {
            threads.put(Thread.currentThread(), ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime());
        } else {
            threads.put(Thread.currentThread(), -1L);
        }
        totalThreads++;
        return this;
    }

    public SearchStatus deattachThread() {
        Thread thread = Thread.currentThread();
        long startingThreadCpuTime = threads.remove(thread);
        long currentThreadCpuTime = bean.getThreadCpuTime(thread.getId());
        cpuTimeOfFinishedThreads.addAndGet(currentThreadCpuTime - startingThreadCpuTime);
        return this;
    }

    public long getUserUuid() {
        return searchQuery.uuid;
    }

    public void cancelSearch() {
        interrupted = true;
        for (Thread t : threads.keySet()) {
            t.interrupt();
        }
    }

    public SearchQueryStatus getSearchQueryStatus() {
        SearchQueryStatus searchQueryStatus = new SearchQueryStatus();
        searchQueryStatus.query = searchQuery;
        if (facet != null) {
            searchQueryStatus.facet = facet;
        }
        searchQueryStatus.complete = getCompleteStatus();
        if (CPU_TIME_SUPPORTED) {
            searchQueryStatus.cpuTime = getCpuTime();
        }
        searchQueryStatus.running = !finished;
        searchQueryStatus.interrupted = interrupted;
        searchQueryStatus.realTime = System.currentTimeMillis() - startingTime;
        return searchQueryStatus;
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
        if (getFinishedTime() + TimeUnit.MINUTES.toMillis(1) < System.currentTimeMillis()) {
            return true;
        }
        return false;
    }
}
