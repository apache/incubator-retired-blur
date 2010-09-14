package com.nearinfinity.blur.manager.hits;

import java.util.Comparator;

import com.nearinfinity.blur.thrift.generated.Hit;

public class HitsComparator implements Comparator<Hit> {

    @Override
    public int compare(Hit o1, Hit o2) {
        int compare = Double.compare(o2.score, o1.score);
        if (compare == 0) {
            return o2.id.compareTo(o1.id);
        }
        return compare;
    }

}
