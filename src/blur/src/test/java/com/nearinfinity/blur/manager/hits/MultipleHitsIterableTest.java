package com.nearinfinity.blur.manager.hits;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.nearinfinity.blur.thrift.generated.Hit;

public class MultipleHitsIterableTest {
    
    @Test
    public void testMultipleHitsIterable() {
        HitsIterableMultiple iterable = new HitsIterableMultiple();
        iterable.addHitsIterable(newHitsIterable(0,0.1,3,2,9,10,2));
        iterable.addHitsIterable(newHitsIterable(7,2,9,1,34,53,12));
        iterable.addHitsIterable(newHitsIterable(4,3));
        iterable.addHitsIterable(newHitsIterable(7,2,34,132));
        iterable.addHitsIterable(newHitsIterable());
        
        for (Hit hit : iterable) {
            System.out.println(hit);
        }
    }

    private HitsIterable newHitsIterable(double... ds) {
        List<Hit> hits = new ArrayList<Hit>();
        for (double d : ds) {
            hits.add(new Hit(UUID.randomUUID().toString() + "-" + Double.toString(d),d,null));
        }
        return new HitsIterableSimple(UUID.randomUUID().toString(), hits);
    }

}
