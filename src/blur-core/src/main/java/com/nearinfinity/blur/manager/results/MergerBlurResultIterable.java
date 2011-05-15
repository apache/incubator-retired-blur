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

package com.nearinfinity.blur.manager.results;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin.Merger;

public class MergerBlurResultIterable implements Merger<BlurResultIterable> {

    private long _minimumNumberOfResults;
    private long _maxQueryTime;

    public MergerBlurResultIterable(BlurQuery blurQuery) {
        _minimumNumberOfResults = blurQuery.minimumNumberOfResults;
        _maxQueryTime = blurQuery.maxQueryTime;
    }

    @Override
    public BlurResultIterable merge(BlurExecutorCompletionService<BlurResultIterable> service) throws Exception {
        BlurResultIterableMultiple iterable = new BlurResultIterableMultiple();
        while (service.getRemainingCount() > 0) {
            Future<BlurResultIterable> future = service.poll(_maxQueryTime, TimeUnit.MILLISECONDS);
            iterable.addBlurResultIterable(future.get());
            if (iterable.getTotalResults() >= _minimumNumberOfResults) {
                return iterable;
            }
        }
        return iterable;
    }

}
