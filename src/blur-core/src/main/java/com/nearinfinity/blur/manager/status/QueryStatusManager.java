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

public class QueryStatusManager {

  private static final Log LOG = LogFactory.getLog(QueryStatusManager.class);
  private static final Object CONSTANT_VALUE = new Object();

  private Timer statusCleanupTimer;
  private long statusCleanupTimerDelay = TimeUnit.SECONDS.toMillis(60);
  private ConcurrentHashMap<QueryStatus, Object> currentQueryStatusCollection = new ConcurrentHashMap<QueryStatus, Object>();

  public void init() {
    statusCleanupTimer = new Timer("Query-Status-Cleanup", true);
    statusCleanupTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          cleanupFinishedQueryStatuses();
        } catch (Exception e) {
          LOG.error("Unknown error while trying to cleanup finished queries.", e);
        }
      }
    }, statusCleanupTimerDelay, statusCleanupTimerDelay);
  }

  public void close() {
    statusCleanupTimer.cancel();
    statusCleanupTimer.purge();
  }

  public QueryStatus newQueryStatus(String table, BlurQuery blurQuery) {
    return addStatus(new QueryStatus(statusCleanupTimerDelay, table, blurQuery).attachThread());
  }

  private QueryStatus addStatus(QueryStatus status) {
    currentQueryStatusCollection.put(status, CONSTANT_VALUE);
    return status;
  }

  public void removeStatus(QueryStatus status) {
    status.setFinished(true);
  }

  private void cleanupFinishedQueryStatuses() {
    LOG.debug("QueryStatus Start count [{0}].", currentQueryStatusCollection.size());
    Iterator<QueryStatus> iterator = currentQueryStatusCollection.keySet().iterator();
    while (iterator.hasNext()) {
      QueryStatus status = iterator.next();
      if (status.isValidForCleanUp()) {
        currentQueryStatusCollection.remove(status);
      }
    }
    LOG.debug("QueryStatus Finish count [{0}].", currentQueryStatusCollection.size());
  }

  public long getStatusCleanupTimerDelay() {
    return statusCleanupTimerDelay;
  }

  public void setStatusCleanupTimerDelay(long statusCleanupTimerDelay) {
    this.statusCleanupTimerDelay = statusCleanupTimerDelay;
  }

  public void cancelQuery(String table, long uuid) {
    for (QueryStatus status : currentQueryStatusCollection.keySet()) {
      if (status.getUserUuid() == uuid && status.getTable().equals(table)) {
        status.cancelQuery();
      }
    }
  }

  public List<BlurQueryStatus> currentQueries(String table) {
    List<BlurQueryStatus> result = new ArrayList<BlurQueryStatus>();
    for (QueryStatus status : currentQueryStatusCollection.keySet()) {
      if (status.getTable().equals(table)) {
        result.add(status.getQueryStatus());
      }
    }
    return result;
  }
}
