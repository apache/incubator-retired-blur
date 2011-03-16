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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;

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
    
    public void close() {
        searchStatusCleanupTimer.cancel();
        searchStatusCleanupTimer.purge();
    }
    
    public SearchStatus newSearchStatus(String table, BlurQuery searchQuery) {
        return addStatus(new SearchStatus(searchStatusCleanupTimerDelay,table,searchQuery).attachThread());
    }
    
    private SearchStatus addStatus(SearchStatus status) {
        currentSearchStatusCollection.put(status,CONSTANT_VALUE);
        return status;
    }
    
    public void removeStatus(SearchStatus status) {
        status.setFinished(true);
    }
    
    private void cleanupFinishedSearchStatuses() {
        LOG.debug("SearchStatus Start count [{0}].",currentSearchStatusCollection.size());
        Iterator<SearchStatus> iterator = currentSearchStatusCollection.keySet().iterator();
        while (iterator.hasNext()) {
            SearchStatus status = iterator.next();
            if (status.isValidForCleanUp()) {
                currentSearchStatusCollection.remove(status);
            }
        }
        LOG.debug("SearchStatus Finish count [{0}].",currentSearchStatusCollection.size());
    }

    public long getSearchStatusCleanupTimerDelay() {
        return searchStatusCleanupTimerDelay;
    }

    public void setSearchStatusCleanupTimerDelay(long searchStatusCleanupTimerDelay) {
        this.searchStatusCleanupTimerDelay = searchStatusCleanupTimerDelay;
    }

    public void cancelSearch(String table, long uuid) {
        for (SearchStatus status : currentSearchStatusCollection.keySet()) {
            if (status.getUserUuid() == uuid && status.getTable().equals(table)) {
                status.cancelSearch();
            }
        }
    }

    public List<BlurQueryStatus> currentSearches(String table) {
        List<BlurQueryStatus> result = new ArrayList<BlurQueryStatus>();
        for (SearchStatus status : currentSearchStatusCollection.keySet()) {
            if (status.getTable().equals(table)) {
                result.add(status.getSearchQueryStatus());
            }
        }
        return result;
    }

}
