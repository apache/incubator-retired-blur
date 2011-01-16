package com.nearinfinity.blur.manager.hits;

import java.util.Map;

import com.nearinfinity.blur.thrift.generated.Hit;

public interface HitsIterable extends Iterable<Hit> {

    void skipTo(long skipTo);

    long getTotalHits();

    Map<String, Long> getShardInfo();

}
