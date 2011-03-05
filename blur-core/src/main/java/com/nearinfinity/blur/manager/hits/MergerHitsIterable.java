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

package com.nearinfinity.blur.manager.hits;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin.Merger;

public class MergerHitsIterable implements Merger<HitsIterable> {

    private long minimumNumberOfHits;
    private long maxQueryTime;

    public MergerHitsIterable(long minimumNumberOfHits, long maxQueryTime) {
        this.minimumNumberOfHits = minimumNumberOfHits;
        this.maxQueryTime = maxQueryTime;
    }

    @Override
    public HitsIterable merge(BlurExecutorCompletionService<HitsIterable> service) throws Exception {
        HitsIterableMultiple iterable = new HitsIterableMultiple();
        while (service.getRemainingCount() > 0) {
            Future<HitsIterable> future = service.poll(maxQueryTime, TimeUnit.MILLISECONDS);
            iterable.addHitsIterable(future.get());
            if (iterable.getTotalHits() >= minimumNumberOfHits) {
                return iterable;
            }
        }
        return iterable;
    }

}
