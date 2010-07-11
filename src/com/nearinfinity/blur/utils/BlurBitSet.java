package com.nearinfinity.blur.utils;

import org.apache.lucene.util.OpenBitSet;

public class BlurBitSet extends OpenBitSet {

	private static final long serialVersionUID = 8837907556056943537L;
	
	public BlurBitSet() {
		super();
	}

	public BlurBitSet(long numBits) {
		super(numBits);
	}

	public BlurBitSet(long[] bits, int numWords) {
		super(bits, numWords);
	}

	public int prevSetBit(int index) {
		return (int) prevSetBit((long)index);
	}
	
	public long prevSetBit(long index) {
		if (index < 0) {
			return -1;
		}
		long bIdx = index >>> 6L;
		int bit = (int) (index & 0x3F);
		int blockIdx = (int) bIdx;
		// < 0 is a check for when the int cast is larger that than an int
		if (blockIdx < 0 || blockIdx > wlen - 1) {
			blockIdx = wlen - 1;
			bit = 63;
		}
		
		while (blockIdx >= 0) {
			long block = bits[blockIdx];
			if (block != 0l) {
				while (bit >= 0) {
					long mask = 1L << bit;
					if ((block & mask) != 0) {
						return getRealPosition(blockIdx,bit);
					}
					bit--;
				}
				bit = 63;
			}
			blockIdx--;
		}
		return -1;
	}

	private long getRealPosition(long block, int position) {
		return (block << 6) + position;
	}
	
	

}
