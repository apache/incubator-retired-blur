package com.nearinfinity.blur.utils;

import java.util.Comparator;

import com.nearinfinity.blur.thrift.generated.ColumnFamily;

public class ColumnFamilyComparator implements Comparator<ColumnFamily> {

    @Override
    public int compare(ColumnFamily o1, ColumnFamily o2) {
        return o1.family.compareTo(o2.family);
    }

}
