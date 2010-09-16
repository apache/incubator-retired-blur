package com.nearinfinity.blur.manager.hits;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.utils.BlurConstants;

public class HitsIterableSimple implements HitsIterable {
    
    private List<Hit> hits;
    private Map<String, Long> shardInfo;
    private long skipTo;

    public HitsIterableSimple(String shard, List<Hit> hits) {
        Collections.sort(hits,BlurConstants.HITS_COMPARATOR);
        this.hits = hits;
        this.shardInfo = new TreeMap<String, Long>();
        this.shardInfo.put(shard, (long) hits.size());
    }

    @Override
    public Map<String, Long> getShardInfo() {
        return shardInfo;
    }

    @Override
    public long getTotalHits() {
        return hits.size();
    }

    @Override
    public void skipTo(long skipTo) {
        this.skipTo = skipTo;
    }

    @Override
    public Iterator<Hit> iterator() {
        long start = 0;
        Iterator<Hit> iterator = hits.iterator();
        while (iterator.hasNext() && start < skipTo) {
            iterator.next();
            start++;
        }
        return iterator;
    }

}
