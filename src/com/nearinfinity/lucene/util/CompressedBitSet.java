package com.nearinfinity.lucene.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.OpenBitSet;

public abstract class CompressedBitSet extends DocIdSet implements Serializable {
	
	private static final long serialVersionUID = 2632337684791004372L;

	public abstract boolean get(int index);
	public abstract void appendSet(int index);
	public abstract int nextSetBit(int index);
	public abstract int prevSetBit(int index);
	
	public static CompressedBitSet create(Iterator<Integer> bitsToSet) {
		return new CompressedBitSetInt(bitsToSet);
	}
	
	public abstract long getMemorySize();
	
	public abstract long getOriginalMemorySize();
	
	public abstract byte[] toBytes() throws IOException;

	public abstract CompressedBitSet toBytes(byte[] bytes) throws IOException;
	
	public static CompressedBitSet create(OpenBitSet bitSet, double threshold) {
		CompressedBitSetInt compressedBitSetInt = new CompressedBitSetInt(bitSet);
		double memorySize = compressedBitSetInt.getMemorySize();
		double originalMemorySize = compressedBitSetInt.getOriginalMemorySize();
		double t = memorySize / originalMemorySize;
		if (t <= threshold) {
			return compressedBitSetInt;
		}
		return new CompressedBitSetNoCompression(bitSet);
	}
}
