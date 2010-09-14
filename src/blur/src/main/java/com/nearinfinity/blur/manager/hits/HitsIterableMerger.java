package com.nearinfinity.blur.manager.hits;

import java.util.List;
import java.util.concurrent.Future;

import com.nearinfinity.blur.utils.ForkJoin.Merger;

public class HitsIterableMerger implements Merger<HitsIterable> {

    private long minimumNumberOfHits;

    public HitsIterableMerger(long minimumNumberOfHits) {
        this.minimumNumberOfHits = minimumNumberOfHits;
    }

    @Override
    public HitsIterable merge(List<Future<HitsIterable>> futures) throws Exception {
        MultipleHitsIterable iterable = new MultipleHitsIterable();
        for (Future<HitsIterable> future : futures) {
            iterable.addHitsIterable(future.get());
            if (iterable.getTotalHits() >= minimumNumberOfHits) {
                return iterable;
            }
        }
        return iterable;
    }

}
