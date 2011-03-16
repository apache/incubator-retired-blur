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
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin.Merger;

public class MergerQueryStatus implements Merger<List<BlurQueryStatus>> {

    @Override
    public List<BlurQueryStatus> merge(BlurExecutorCompletionService<List<BlurQueryStatus>> service) throws Exception {
        Map<Long,BlurQueryStatus> statusMap = new HashMap<Long,BlurQueryStatus>();
        while (service.getRemainingCount() > 0) {
            Future<List<BlurQueryStatus>> future = service.take();
            addToMap(statusMap,future.get());
        }
        return new ArrayList<BlurQueryStatus>(statusMap.values());
    }

    private void addToMap(Map<Long, BlurQueryStatus> statusMap, List<BlurQueryStatus> list) {
        for (BlurQueryStatus status : list) {
            BlurQueryStatus searchQueryStatus = statusMap.get(status.uuid);
            if (searchQueryStatus == null) {
                statusMap.put(status.uuid, status);
            } else {
                statusMap.put(status.uuid, merge(searchQueryStatus,status));
            }
        }
    }

    private BlurQueryStatus merge(BlurQueryStatus s1, BlurQueryStatus s2) {
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
