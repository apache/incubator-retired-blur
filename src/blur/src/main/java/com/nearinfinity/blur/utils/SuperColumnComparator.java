package com.nearinfinity.blur.utils;

import java.util.Comparator;

import com.nearinfinity.blur.thrift.generated.SuperColumn;

public class SuperColumnComparator implements Comparator<SuperColumn> {

	@Override
	public int compare(SuperColumn o1, SuperColumn o2) {
		int family = o1.family.compareTo(o2.family);
		if (family == 0) {
			return o1.id.compareTo(o2.id);
		}
		return family;
	}

}
