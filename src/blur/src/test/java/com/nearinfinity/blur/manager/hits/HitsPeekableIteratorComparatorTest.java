package com.nearinfinity.blur.manager.hits;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.utils.BlurConstants;

public class HitsPeekableIteratorComparatorTest {
    
    @Test
    public void testHitsPeekableIteratorComparator() {
        List<PeekableIterator<Hit>> hits = new ArrayList<PeekableIterator<Hit>>();
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>(Arrays.asList(newHit("5",5))).iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>().iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>().iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>(Arrays.asList(newHit("2",2))).iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>(Arrays.asList(newHit("1",1))).iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>(Arrays.asList(newHit("9",1))).iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>().iterator()));
        
        Collections.sort(hits,BlurConstants.HITS_PEEKABLE_ITERATOR_COMPARATOR);
        
        for (PeekableIterator<Hit> iterator : hits) {
            System.out.println(iterator.peek());
        }
    }

    private Hit newHit(String id, double score) {
        return new Hit(id, score, null);
    }

}
