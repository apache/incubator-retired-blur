package com.nearinfinity.blur.manager.hits;

import java.util.Comparator;

import com.nearinfinity.blur.thrift.generated.Hit;

public class HitsPeekableIteratorComparator implements Comparator<PeekableIterator<Hit>> {

    @Override
    public int compare(PeekableIterator<Hit> o1, PeekableIterator<Hit> o2) {
        Hit hit1 = o1.peek();
        Hit hit2 = o2.peek();
        if (hit1 == null && hit2 == null) {
            return 0;
        } else if (hit1 == null) {
            return 1;
        } else if (hit2 == null) {
            return -1;
        }
        int compare = Double.compare(hit2.score, hit1.score);
        if (compare == 0) {
            return hit2.id.compareTo(hit1.id);
        }
        return compare;
    }

}
