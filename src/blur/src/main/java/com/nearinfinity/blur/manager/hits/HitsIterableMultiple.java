package com.nearinfinity.blur.manager.hits;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.utils.BlurConstants;

public class HitsIterableMultiple implements HitsIterable {
    
    private long totalHits;
    private Map<String, Long> shardInfo = new TreeMap<String, Long>();
    private long skipTo;
    private List<HitsIterable> hits = new ArrayList<HitsIterable>();

    public void addHitsIterable(HitsIterable iterable) {
        totalHits += iterable.getTotalHits();
        shardInfo.putAll(iterable.getShardInfo());
        hits.add(iterable);
    }

    @Override
    public Map<String, Long> getShardInfo() {
        return shardInfo;
    }

    @Override
    public long getTotalHits() {
        return totalHits;
    }

    @Override
    public void skipTo(long skipTo) {
        this.skipTo = skipTo;
    }

    @Override
    public Iterator<Hit> iterator() {
        MultipleHitsIterator iterator = new MultipleHitsIterator(hits);
        long start = 0;
        while (iterator.hasNext() && start < skipTo) {
            iterator.next();
            start++;
        }
        return iterator;
    }
    
    public static class MultipleHitsIterator implements Iterator<Hit> {
        
        private List<PeekableIterator<Hit>> iterators = new ArrayList<PeekableIterator<Hit>>();
        private int length;

        public MultipleHitsIterator(List<HitsIterable> hits) {
            for (HitsIterable hitsIterable : hits) {
                iterators.add(new PeekableIterator<Hit>(hitsIterable.iterator()));
            }
            length = iterators.size();
        }

        @Override
        public boolean hasNext() {
            for (int i = 0; i < length; i++) {
                if (iterators.get(i).hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Hit next() {
            Collections.sort(iterators, BlurConstants.HITS_PEEKABLE_ITERATOR_COMPARATOR);
            return iterators.get(0).next();
        }

        @Override
        public void remove() {

        }
    }
}
