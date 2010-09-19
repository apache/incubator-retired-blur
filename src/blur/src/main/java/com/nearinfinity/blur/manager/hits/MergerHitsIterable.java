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
