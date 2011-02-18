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
        //do not use yet
        
        if (index < 0) {
            return -1;
        }

        int i = (int) (index >>> 6);
        if (i == -1) {
            i = Integer.MAX_VALUE;
        }
        if (i < wlen) {
//            int subIndex = (int) index & 0x3f;
//            int shiftSize = 64 - subIndex - 1;
//            long word = bits[i] >>> subIndex;
//            word = word << (shiftSize + subIndex);
//            if (word != 0) {
//                return (((long) i) << 6) + (BitUtil.ntz(word) - shiftSize);
//            }
            
            int subIndex = (int) index & 0x3f;
            long word = Long.reverse(bits[i]) >>> subIndex;
//            System.out.println(buffer(Long.toBinaryString(word)));
            if (word!=0) {
                System.out.println(buffer(Long.toBinaryString(word)));
                System.out.println(buffer(Long.toBinaryString(Long.reverse(word))));
                
                return (((long)i)<<6) + BitUtil.ntz(Long.reverse(word)) - subIndex;
              }
            
        } else {
            i = wlen;
        }

        while (--i >= 0) {
            long word = bits[i];
            if (word != 0) {
                return (((long) i) << 6) + (64 - BitUtil.ntz(Long.reverse(word)) - 1);
            }
        }
        return -1;
    }

    private String buffer(String s) {
        while (s.length() < 64) {
            s = "0" + s;
        }
        return s;
    }
}
