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
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.QueryState;
import org.apache.blur.thrift.generated.User;
import org.apache.blur.utils.GCAction;
import org.apache.blur.utils.GCWatcher;

public class QueryStatusManager implements Closeable {

  private static final Log LOG = LogFactory.getLog(QueryStatusManager.class);
  private static final Object CONSTANT_VALUE = new Object();

  private final Timer _statusCleanupTimer;
  private final long _statusCleanupTimerDelay;
  private final ConcurrentMap<QueryStatus, Object> _currentQueryStatusCollection = new ConcurrentHashMap<QueryStatus, Object>();
  
  public QueryStatusManager(long statusCleanupTimerDelay) {
    _statusCleanupTimerDelay = statusCleanupTimerDelay;
    _statusCleanupTimer = new Timer("Query-Status-Cleanup", true);
    _statusCleanupTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          cleanupFinishedQueryStatuses();
        } catch (Throwable e) {
          LOG.error("Unknown error while trying to cleanup finished queries.", e);
        }
      }
    }, _statusCleanupTimerDelay, _statusCleanupTimerDelay);
    GCWatcher.registerAction(new GCAction() {
      @Override
      public void takeAction() throws Exception {
        stopAllQueriesForBackPressure();
      }
    });
  }

  @Override
  public void close() {
    _statusCleanupTimer.cancel();
    _statusCleanupTimer.purge();
  }

  public QueryStatus newQueryStatus(String table, BlurQuery blurQuery, int maxNumberOfThreads, AtomicBoolean running, User user) {
    QueryStatus queryStatus = new QueryStatus(_statusCleanupTimerDelay, table, blurQuery, running, user);
    _currentQueryStatusCollection.put(queryStatus, CONSTANT_VALUE);
    return queryStatus;
  }

  public void removeStatus(QueryStatus status) {
    status.setFinished(true);
  }

  private void cleanupFinishedQueryStatuses() {
    LOG.debug("QueryStatus Start count [{0}].", _currentQueryStatusCollection.size());
    Iterator<QueryStatus> iterator = _currentQueryStatusCollection.keySet().iterator();
    while (iterator.hasNext()) {
      QueryStatus status = iterator.next();
      if (status.isValidForCleanUp()) {
        _currentQueryStatusCollection.remove(status);
      }
    }
    LOG.debug("QueryStatus Finish count [{0}].", _currentQueryStatusCollection.size());
  }

  public long getStatusCleanupTimerDelay() {
    return _statusCleanupTimerDelay;
  }

  public void cancelQuery(String table, String uuid) {
    for (QueryStatus status : _currentQueryStatusCollection.keySet()) {
      String userUuid = status.getUserUuid();
      if (userUuid != null && userUuid.equals(uuid) && status.getTable().equals(table)) {
        status.cancelQuery();
      }
    }
  }

  public List<BlurQueryStatus> currentQueries(String table) {
    List<BlurQueryStatus> result = new ArrayList<BlurQueryStatus>();
    for (QueryStatus status : _currentQueryStatusCollection.keySet()) {
      if (status.getTable().equals(table)) {
        result.add(status.getQueryStatus());
      }
    }
    return result;
  }

  public BlurQueryStatus queryStatus(String table, String uuid) {
    for (QueryStatus status : _currentQueryStatusCollection.keySet()) {
      String userUuid = status.getUserUuid();
      if (userUuid != null && userUuid.equals(uuid) && status.getTable().equals(table)) {
        return status.getQueryStatus();
      }
    }
    return null;
  }

  public List<String> queryStatusIdList(String table) {
    Set<String> ids = new HashSet<String>();
    for (QueryStatus status : _currentQueryStatusCollection.keySet()) {
      if (status.getTable().equals(table)) {
        if (status.getUserUuid() != null) {
          ids.add(status.getUserUuid());  
        }
      }
    }
    return new ArrayList<String>(ids);
  }

  public void stopAllQueriesForBackPressure() {
    LOG.warn("Stopping all queries for back pressure.");
    for (QueryStatus status : _currentQueryStatusCollection.keySet()) {
      QueryState state = status.getQueryStatus().getState();
      if (state == QueryState.RUNNING) {
        status.stopQueryForBackPressure();
      }
    }
  }
}
