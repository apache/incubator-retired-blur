package com.nearinfinity.blur.utils;

import java.util.Comparator;

import com.nearinfinity.blur.thrift.generated.Column;

public class ColumnComparator implements Comparator<Column> {

	@Override
	public int compare(Column o1, Column o2) {
		return o1.name.compareTo(o2.name);
	}

}
