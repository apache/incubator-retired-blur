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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.nearinfinity.blur.thrift.generated.BlurResult;
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
    public Iterator<BlurResult> iterator() {
        MultipleHitsIterator iterator = new MultipleHitsIterator(hits);
        long start = 0;
        while (iterator.hasNext() && start < skipTo) {
            iterator.next();
            start++;
        }
        return iterator;
    }
    
    public static class MultipleHitsIterator implements Iterator<BlurResult> {
        
        private List<PeekableIterator<BlurResult>> iterators = new ArrayList<PeekableIterator<BlurResult>>();
        private int length;

        public MultipleHitsIterator(List<HitsIterable> hits) {
            for (HitsIterable hitsIterable : hits) {
                iterators.add(new PeekableIterator<BlurResult>(hitsIterable.iterator()));
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
        public BlurResult next() {
            Collections.sort(iterators, BlurConstants.HITS_PEEKABLE_ITERATOR_COMPARATOR);
            return iterators.get(0).next();
        }

        @Override
        public void remove() {

        }
    }
}
