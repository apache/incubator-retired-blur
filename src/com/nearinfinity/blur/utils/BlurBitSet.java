package com.nearinfinity.blur.utils;

import org.apache.lucene.util.OpenBitSet;

public class BlurBitSet extends OpenBitSet {

	private static final long serialVersionUID = 8837907556056943537L;

	public int prevSetBit(int index) {
		return (int) prevSetBit((long)index);
	}

	public long prevSetBit(long index) {
		while (!fastGet(index) && index >= 0) {
			index--;
		}
		return index;
	}
	
	

}
