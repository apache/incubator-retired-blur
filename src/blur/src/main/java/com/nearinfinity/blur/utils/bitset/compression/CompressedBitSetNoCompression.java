package com.nearinfinity.blur.utils.bitset.compression;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.OpenBitSet;

public class CompressedBitSetNoCompression extends CompressedBitSet {
	
	private static final long serialVersionUID = 5182862892902526856L;
	private OpenBitSet bitSet;

	public CompressedBitSetNoCompression(OpenBitSet bitSet) {
		this.bitSet = bitSet;
	}
	
	@Override
	public void appendSet(int index) {
		throw new RuntimeException();
	}

	@Override
	public int nextSetBit(int index) {
		throw new RuntimeException();
	}

	@Override
	public int prevSetBit(int index) {
		throw new RuntimeException();
	}
	
	@Override
	public boolean get(int index) {
		return bitSet.get(index);
	}

	@Override
	public byte[] toBytes() throws IOException {
		throw new RuntimeException();
	}

	@Override
	public CompressedBitSet toBytes(byte[] bytes) throws IOException {
		throw new RuntimeException();
	}

	@Override
	public DocIdSetIterator iterator() throws IOException {
		return bitSet.iterator();
	}

	@Override
	public long getMemorySize() {
		return bitSet.getBits().length * 8;
	}

	@Override
	public long getOriginalMemorySize() {
		return getMemorySize();
	}

}
