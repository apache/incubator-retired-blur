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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.CpuTime;
import com.nearinfinity.blur.thrift.generated.QueryState;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin.Merger;

public class MergerQueryStatus implements Merger<List<BlurQueryStatus>> {

  @Override
  public List<BlurQueryStatus> merge(BlurExecutorCompletionService<List<BlurQueryStatus>> service) throws Exception {
    Map<Long, BlurQueryStatus> statusMap = new HashMap<Long, BlurQueryStatus>();
    while (service.getRemainingCount() > 0) {
      Future<List<BlurQueryStatus>> future = service.take();
      addToMap(statusMap, future.get());
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

  private BlurQueryStatus merge(BlurQueryStatus s1, BlurQueryStatus s2) {
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
    s1.cpuTimes.putAll(s2.cpuTimes);
    return s1;
  }

}
