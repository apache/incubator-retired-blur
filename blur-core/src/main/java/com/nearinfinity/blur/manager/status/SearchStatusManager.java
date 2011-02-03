package com.nearinfinity.blur.manager.status;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.nearinfinity.blur.thrift.generated.Facet;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;

public class SearchStatusManager {
    
    private static final Log LOG = LogFactory.getLog(SearchStatusManager.class);
    private static final Object CONSTANT_VALUE = new Object();
    
    private Timer searchStatusCleanupTimer;
    private long searchStatusCleanupTimerDelay = TimeUnit.SECONDS.toMillis(60);
    private ConcurrentHashMap<SearchStatus, Object> currentSearchStatusCollection = new ConcurrentHashMap<SearchStatus, Object>();

    public void init() {
        searchStatusCleanupTimer = new Timer("Search-Status-Cleanup",true);
        searchStatusCleanupTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    cleanupFinishedSearchStatuses();
                } catch (Exception e) {
                    LOG.error("Unknown error while trying to cleanup finished searches.",e);
                }
            }
        }, searchStatusCleanupTimerDelay, searchStatusCleanupTimerDelay);
    }
    
    public void close() throws InterruptedException {
        searchStatusCleanupTimer.cancel();
        searchStatusCleanupTimer.purge();
    }
    
    public SearchStatus newSearchStatus(String table, SearchQuery searchQuery) {
        return addStatus(new SearchStatus(searchStatusCleanupTimerDelay,table,searchQuery).attachThread());
    }
    
    public SearchStatus newSearchStatus(String table, SearchQuery searchQuery, Facet facet) {
        return addStatus(new SearchStatus(searchStatusCleanupTimerDelay,table,searchQuery, facet).attachThread());
    }
    
    private SearchStatus addStatus(SearchStatus status) {
        currentSearchStatusCollection.put(status,CONSTANT_VALUE);
        return status;
    }
    
    public void removeStatus(SearchStatus status) {
        status.setFinished(true);
    }
    
    private void cleanupFinishedSearchStatuses() {
        LOG.debug("SearchStatus Start count [" + currentSearchStatusCollection.size() + "].");
        Iterator<SearchStatus> iterator = currentSearchStatusCollection.keySet().iterator();
        while (iterator.hasNext()) {
            SearchStatus status = iterator.next();
            if (status.isValidForCleanUp()) {
                currentSearchStatusCollection.remove(status);
            }
        }
        LOG.debug("SearchStatus Finish count [" + currentSearchStatusCollection.size() + "].");
    }

    public long getSearchStatusCleanupTimerDelay() {
        return searchStatusCleanupTimerDelay;
    }

    public void setSearchStatusCleanupTimerDelay(long searchStatusCleanupTimerDelay) {
        this.searchStatusCleanupTimerDelay = searchStatusCleanupTimerDelay;
    }

    public void cancelSearch(long uuid) {
        for (SearchStatus status : currentSearchStatusCollection.keySet()) {
            if (status.getUserUuid() == uuid) {
                status.cancelSearch();
            }
        }
    }

    public List<SearchQueryStatus> currentSearches(String table) {
        List<SearchQueryStatus> result = new ArrayList<SearchQueryStatus>();
        for (SearchStatus status : currentSearchStatusCollection.keySet()) {
            if (status.getTable().equals(table)) {
                result.add(status.getSearchQueryStatus());
            }
        }
        return result;
    }

}
