package com.nearinfinity.blur.utils;

import static junit.framework.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.nearinfinity.blur.utils.bitset.BlurBitSet;

public class BlurBitSetTest {

    @Test
	public void testRandomBlurBitSet() {
		BlurBitSet blurBitSet = new BlurBitSet();
		long seed = getSeed();
		System.out.println("Running with seed [" + seed + "]");
        populate(blurBitSet,1000000,100000,new Random(seed));
		
		List<Long> setBits = new ArrayList<Long>();
		long bit = -1;
		while ((bit = blurBitSet.nextSetBit(bit + 1)) != -1) {
			setBits.add(bit);
		}
		
		bit = Long.MAX_VALUE;
		while ((bit = blurBitSet.prevSetBit(bit - 1)) != -1) {
			if (!setBits.remove((Long)bit)) {
				fail("Bit [" + Long.toString(bit) + "] was not found.");
			}
		}
	}
	
    private long getSeed() {
        return new Random().nextLong();
    }

    @Test
	public void testPerformance() {
		BlurBitSet blurBitSet = new BlurBitSet();
		long seed = getSeed();
        System.out.println("Running with seed [" + seed + "]");
		populate(blurBitSet,1000000,10000,new Random(seed));
		
		long nextTime = 0;
		long prevTime = 0;
		
		for (int i = 0; i < 100; i++) {
			long total1 = 0;
			long bit = -1;
			long s1 = System.nanoTime();
			while ((bit = blurBitSet.nextSetBit(bit + 1)) != -1) {
				total1 += bit;
			}
			long e1 = System.nanoTime();
			nextTime += (e1-s1);
			
			long total2 = 0;
			bit = Long.MAX_VALUE;
			long s2 = System.nanoTime();
			while ((bit = blurBitSet.prevSetBit(bit - 1)) != -1) {
				total2 += bit;
			}
			long e2 = System.nanoTime();
			prevTime += (e2-s2);
		}
		
		System.out.println("Next time [" + nextTime +
				"] Prev Time [" + prevTime +
				"]");
	}

	private void populate(BlurBitSet blurBitSet, int maxSize, int maxPopulation, Random random) {
		while (blurBitSet.cardinality() < maxPopulation) {
			blurBitSet.set(random.nextInt(maxSize));
		}
	}
	
}
