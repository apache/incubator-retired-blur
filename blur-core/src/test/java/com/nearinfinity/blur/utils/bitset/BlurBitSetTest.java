package com.nearinfinity.blur.utils.bitset;

import static junit.framework.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.util.BitUtil;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.utils.bitset.BlurBitSet;

public class BlurBitSetTest {
	
	
	private BlurBitSet bits;

	@Before
	public void setUp() {
		bits = new BlurBitSet();
	}

	@Test
	public void testPreviousHitBeginning() {
		assertEquals(-1, bits.prevSetBit(30));
		bits.set(50);
		assertEquals(-1, bits.prevSetBit(30));
	}

	@Test
	public void testPreviousBlurBitSet() {
		bits.set(10);
		assertEquals(10, bits.prevSetBit(14));
	}

	@Test
	public void testPreviousSmallBitLargeSearch() {
		bits.set(10);
		assertEquals(10, bits.prevSetBit(5000));
	}

	@Test
	public void testPreviousLargeBit() {
		bits.set(200);
		assertEquals(200, bits.prevSetBit(400));
		assertEquals(-1, bits.prevSetBit(100));
	}

	@Test
	public void testSmallRandomBlurBitSet() {
		long seed = getSeed();
		System.out.println("testSmallRandomBlurBitSet Running with seed [" + seed + "]");
		populate(bits, 100, 10, new Random(seed));

		List<Long> setBits = new ArrayList<Long>();
		long bit = -1;
		while ((bit = bits.nextSetBit(bit + 1)) != -1) {
			setBits.add(bit);
		}

		bit = Long.MAX_VALUE;
		while ((bit = bits.prevSetBit(bit - 1)) != -1) {
			if (!setBits.remove((Long) bit)) {
				fail("Bit [" + Long.toString(bit) + "] was not found.");
			}
		}
		assertEquals(0, setBits.size());
	}
	@Test
	public void testRandomBlurBitSet() {
		long seed = getSeed();
		System.out.println("testRandomBlurBitSet Running with seed [" + seed + "]");
		populate(bits, 1000000, 10000, new Random(seed));

		List<Long> setBits = new ArrayList<Long>();
		long bit = -1;
		while ((bit = bits.nextSetBit(bit + 1)) != -1) {
			setBits.add(bit);
		}

		bit = Long.MAX_VALUE;
		while ((bit = bits.prevSetBit(bit - 1)) != -1) {
			if (!setBits.remove((Long) bit)) {
				fail("Bit [" + Long.toString(bit) + "] was not found.");
			}
		}
		assertEquals(0, setBits.size());
	}

	private long getSeed() {
		return new Random().nextLong();
	}

	@Test
	public void testPerformance() {
		long seed = getSeed();
		System.out.println("testPerformance Running with seed [" + seed + "]");
		populate(bits, 1000000, 1000, new Random(seed));

		long nextTime = 0;
		long prevTime = 0;

		for (int i = 0; i < 100; i++) {
			long total1 = 0;
			long bit = -1;
			long s1 = System.nanoTime();
			while ((bit = bits.nextSetBit(bit + 1)) != -1) {
				total1 += bit;
			}
			long e1 = System.nanoTime();
			nextTime += (e1 - s1);

			long total2 = 0;
			bit = Long.MAX_VALUE;
			long s2 = System.nanoTime();
			while ((bit = bits.prevSetBit(bit - 1)) != -1) {
				total2 += bit;
			}
			long e2 = System.nanoTime();
			prevTime += (e2 - s2);
			System.out.print('.');
			assertEquals(total1, total2);
		}
		System.out.println("");

		System.out.println("Next time [" + nextTime + "] Prev Time ["
				+ prevTime + "]");
	}

	@Test
	public void testCount() {
		assertEquals(2, countOldWay(3452473186819272400l));
		assertEquals(2, BlurBitSet.countLeftZeros(3452473186819272400l));
	}
	@Test
	public void testCountLeftZerosPerformance() {
		int arrayCount=500000;
		long[] ls = new long[arrayCount];
		long seed = getSeed();
		
		System.out.println("testCountLeftZerosPerformance Running with seed [" + seed + "]");
		Random rand = new Random(seed);
		for(int i = 0; i<arrayCount; i++){
			ls[i] = rand.nextLong();
		}
		
		long newCounter = 0;
		long s2 = System.nanoTime();
		for(int i = 0; i < arrayCount; i++	){
			newCounter += BlurBitSet.countLeftZeros(ls[i]);
		}
		long e2 = System.nanoTime();

		
		long oldCounter = 0;
		long s1 = System.nanoTime();
		for(int i = 0; i < arrayCount; i++	){
			oldCounter += countOldWay(ls[i]);
		}
		long e1 = System.nanoTime();
		
		
		System.out.println("old way took " + (e1-s1));
		System.out.println("new way took " + (e2-s2));
		System.out.println("new way is " + ((float)(e1-s1) / (float)(e2-s2)) + " times faster");
		for(int i = 0; i < arrayCount; i++) {
			assertEquals(i + " for number [" + ls[i] + "]", countOldWay(ls[i]), BlurBitSet.countLeftZeros(ls[i]));
		}
	}
	
	private int countOldWay(long word) {
		word = Long.reverse(word);
		int count = BitUtil.ntz(word);
		return count;
	}
	
	
	
	private void populate(BlurBitSet blurBitSet, int maxSize,
			int maxPopulation, Random random) {
		while (blurBitSet.cardinality() < maxPopulation) {
			blurBitSet.set(random.nextInt(maxSize));
		}
	}

}
