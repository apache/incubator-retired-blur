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

import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin.Merger;

public class MergerSearchQueryStatus implements Merger<List<SearchQueryStatus>> {

    @Override
    public List<SearchQueryStatus> merge(BlurExecutorCompletionService<List<SearchQueryStatus>> service) throws Exception {
        Map<Long,SearchQueryStatus> statusMap = new HashMap<Long,SearchQueryStatus>();
        while (service.getRemainingCount() > 0) {
            Future<List<SearchQueryStatus>> future = service.take();
            addToMap(statusMap,future.get());
        }
        return new ArrayList<SearchQueryStatus>(statusMap.values());
    }

    private void addToMap(Map<Long, SearchQueryStatus> statusMap, List<SearchQueryStatus> list) {
        for (SearchQueryStatus status : list) {
            SearchQueryStatus searchQueryStatus = statusMap.get(status.uuid);
            if (searchQueryStatus == null) {
                statusMap.put(status.uuid, status);
            } else {
                statusMap.put(status.uuid, merge(searchQueryStatus,status));
            }
        }
    }

    private SearchQueryStatus merge(SearchQueryStatus s1, SearchQueryStatus s2) {
        s1.complete = avg(s1.complete,s2.complete);
        s1.cpuTime = s1.cpuTime + s2.cpuTime;
        s1.interrupted = s1.interrupted || s2.interrupted;
        s1.realTime = s1.realTime + s2.realTime;
        s1.running = s1.running || s2.running;
        return s1;
    }

    private double avg(double... ds) {
        double result = 0.0;
        for (int i = 0; i < ds.length; i++) {
            result += ds[i];
        }
        return result / (double) ds.length;
    }

}
