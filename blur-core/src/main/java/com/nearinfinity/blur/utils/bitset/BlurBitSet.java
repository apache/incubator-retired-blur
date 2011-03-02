package com.nearinfinity.blur.utils.bitset;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.OpenBitSet;

public class BlurBitSet extends OpenBitSet {

    private static final long serialVersionUID = 8837907556056943537L;

    public static void main(String[] args) {

        BlurBitSet bitSet = new BlurBitSet();
        bitSet.set(0);
        bitSet.set(3);
        bitSet.set(5);

        System.out.println(bitSet.prevSetBit(2));
        
//        System.out.println(bitSet.prevSetBit(65));

        for (int i = 0; i < 70; i++) {
            System.out.println(i + " " + bitSet.prevSetBit(i));
        }

    }

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
        return (int) prevSetBit((long) index);
    }
    public long prevSetBit(long index) {
    	if (index < 0) {
            return -1;
        }
    	int initialWordNum = (int)(index >> 6);
    	if (initialWordNum == -1) {
    		initialWordNum = Integer.MAX_VALUE;
        }
    	int wordNum = initialWordNum;
    	if(wordNum > wlen - 1) {
    		wordNum = wlen - 1;
    	}
    	while(wordNum >= 0) {
    		long word = bits[wordNum];
    		if(word != 0) {
    			if(wordNum == initialWordNum){
    				int offset = ((int)(index & 0x3F));
    				long mask = -1L >>> (63-offset);
    				word = word & mask;
    			}
    			if(word != 0) {
    				return (((long)wordNum)<<6) + (63-BitUtil.ntz(Long.reverse(word)));
    			}
    		}
    		wordNum--;
    	}
    	return -1;
    }
}
