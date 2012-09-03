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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.CpuTime;
import org.apache.blur.thrift.generated.QueryState;
import org.apache.blur.utils.BlurExecutorCompletionService;
import org.apache.blur.utils.ForkJoin.Merger;


public class MergerQueryStatus implements Merger<List<BlurQueryStatus>> {

  private long _timeout;

  public MergerQueryStatus(long timeout) {
    _timeout = timeout;
  }

  @Override
  public List<BlurQueryStatus> merge(BlurExecutorCompletionService<List<BlurQueryStatus>> service) throws BlurException {
    Map<Long, BlurQueryStatus> statusMap = new HashMap<Long, BlurQueryStatus>();
    while (service.getRemainingCount() > 0) {
      Future<List<BlurQueryStatus>> future = service.poll(_timeout, TimeUnit.MILLISECONDS, true);
      List<BlurQueryStatus> status = service.getResultThrowException(future);
      addToMap(statusMap, status);
    }
    return new ArrayList<BlurQueryStatus>(statusMap.values());
  }

  private void addToMap(Map<Long, BlurQueryStatus> statusMap, List<BlurQueryStatus> list) {
    for (BlurQueryStatus status : list) {
      BlurQueryStatus searchQueryStatus = statusMap.get(status.uuid);
      if (searchQueryStatus == null) {
        statusMap.put(status.uuid, status);
      } else {
        statusMap.put(status.uuid, merge(searchQueryStatus, status));
      }
    }
  }

  public static BlurQueryStatus merge(BlurQueryStatus s1, BlurQueryStatus s2) {
    s1.completeShards = s1.completeShards + s2.completeShards;
    s1.totalShards = s1.totalShards + s2.totalShards;
    if (s1.state != s2.state) {
      if (s1.state == QueryState.INTERRUPTED || s2.state == QueryState.INTERRUPTED) {
        s1.state = QueryState.INTERRUPTED;
      } else if (s1.state == QueryState.RUNNING || s2.state == QueryState.RUNNING) {
        s1.state = QueryState.RUNNING;
      } else {
        s1.state = QueryState.COMPLETE;
      }
    }
    if (s1.cpuTimes == null) {
      s1.cpuTimes = new HashMap<String, CpuTime>();
    }
    if (s2.cpuTimes != null) {
      s1.cpuTimes.putAll(s2.cpuTimes);
    }
    return s1;
  }

}
