package com.nearinfinity.lucene.util;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.OpenBitSet;

public class CompressedBitSetLong extends CompressedBitSet {

	private static final long serialVersionUID = -6860633393213822190L;
	private static final long ALL_ZEROS = 0; // 0x0000000000000000l
	private static final long ALL_ONES = -1; // 0xFFFFFFFFFFFFFFFFl
	private static final long SET_MSB = Long.MIN_VALUE; // 0x8000000000000000l
	private static final long NOT_SET_MSB = 0x7FFFFFFFFFFFFFFFl;
	private long[] words;
	private OpenBitSet compressedBits = new OpenBitSet();

	public CompressedBitSetLong(OpenBitSet bitSet) {
		compress(bitSet);
	}

	public long getMemorySize() {
		return (words.length + compressedBits.getBits().length) * 8;
	}

	public boolean get(int index) {
		int neededLogicalCursorPosition = index >> 6;
		long logicalCursor = 0;
		for (int i = 0; i < words.length; i++) {
			if (logicalCursor > neededLogicalCursorPosition) {
				return isMsbSet(words[i - 1]);
			} else if (logicalCursor == neededLogicalCursorPosition) {
				if (isCompressedWordFast(i)) {
					return isMsbSet(words[i]);
				}
				// check literal bit
				return (words[i] & (1L << (index & 0x3f))) != 0;
			}
			if (isCompressedWordFast(i)) { // move logical cursor by number of
				// filled words
				logicalCursor += words[i] & NOT_SET_MSB;
			} else {
				logicalCursor++;
			}
		}
		return false;
	}

	private boolean isCompressedWordFast(int wordIndex) {
		return compressedBits.fastGet(wordIndex);
	}

	private boolean isMsbSet(long word) {
		if ((word & SET_MSB) != 0) {
			return true;
		}
		return false;
	}

	private void compress(OpenBitSet bitSet) {
		long[] bits = bitSet.getBits();
		checkNumberOfFilledWords(bits);
		long[] compBits = new long[bits.length];
		int indexOfCompression = 0;
		for (int i = 0; i < bits.length; i++) {
			long l = bits[i];
			if (l == ALL_ZEROS) {
				// compress zeros
				compBits[indexOfCompression] = compBits[indexOfCompression] + 1;
				if (i + 1 < bits.length) {
					if (bits[i + 1] != ALL_ZEROS) {
						compressedBits.set(indexOfCompression);
						indexOfCompression++;
					}
				} else {
					compressedBits.set(indexOfCompression);
					indexOfCompression++;
				}
			} else if (l == ALL_ONES) {
				// compress zeros
				compBits[indexOfCompression] = compBits[indexOfCompression] + 1;
				if (i + 1 < bits.length) {
					if (bits[i + 1] != ALL_ONES) {
						compressedBits.set(indexOfCompression);
						compBits[indexOfCompression] = compBits[indexOfCompression] | SET_MSB;
						indexOfCompression++;
					}
				} else {
					compressedBits.set(indexOfCompression);
					compBits[indexOfCompression] = compBits[indexOfCompression] | SET_MSB;
					indexOfCompression++;
				}
			} else {
				compBits[indexOfCompression++] = l;
			}
		}
		words = new long[indexOfCompression];
		System.arraycopy(compBits, 0, words, 0, indexOfCompression);
	}

	private void checkNumberOfFilledWords(long[] bits) {
		int filledCount = 0;
		int emptyCount = 0;
		for (int i = 0; i < bits.length; i++) {
			if (bits[i] == ALL_ONES) {
				filledCount++;
			}
			if (bits[i] == ALL_ZEROS) {
				emptyCount++;
			}
		}
//		System.out.println("Total Filled words [" + filledCount + "]");
//		System.out.println("Total Empty words [" + emptyCount + "]");
	}


	@Override
	public DocIdSetIterator iterator() throws IOException {
		return new DocIdSetIterator() {

			private int doc = -1;
			private int length = words.length;
			private int bit = 0;
			private int[] reportBits = new int[64];
			private int reportBitLength = 0;
			private int blockReportLength = 0;
			private int wordCounter = 0;
			private int reportCounter = 0;
			private int blockCounter = 0;

			@Override
			public int nextDoc() throws IOException {
				for (; wordCounter < length; wordCounter++) {
					
					if (finishReportingLastBlocks() != -1) return doc;
					
					long word = words[wordCounter];
					
					if (compressedBits.get(wordCounter)) {
						long bitCount = (word & NOT_SET_MSB) << 6;
						if ((word & SET_MSB) == 0) {//false block
							bit += bitCount;
						} else {//true block
							blockReportLength = (int) bitCount;
						}
					} else {
						INNER:
						while (word != 0) {
							int ntz = BitUtil.ntz(word);
							bit += ntz;
							reportBits[reportBitLength++] = bit;
							bit++;
							if (ntz == 63) break INNER;
							word = word >>> (ntz + 1);
						}
						//moves bit to beginning of next word
						bit = ((bit - 1 >> 6l) + 1) << 6l;
					}
				}
				
				if (finishReportingLastBlocks() != -1) return doc;
				
				return noMoreDocs();
			}
			
			private int finishReportingLastBlocks() {
				while (reportCounter < reportBitLength) {
					return doc = reportBits[reportCounter++];
				}
				reportCounter = 0;
				reportBitLength = 0;
				while (blockCounter < blockReportLength) {
					doc = bit;
					bit++;
					blockCounter++;
					return doc;
				}
				blockCounter = 0;
				blockReportLength = 0;
				return -1;
			}

			@Override
			public int docID() {
				return doc;
			}

			@Override
			public int advance(int target) throws IOException {
				int d;
				while ((d = nextDoc()) < target) {}
				return d;
			}

			private int noMoreDocs() {
				return doc = NO_MORE_DOCS;
			}
		};
	}

	
	public void debug(DocIdSetIterator iterator) throws IOException {
		int length = words.length;
		int bit = 0;
		int[] reportBits = new int[64];
		int reportBitLength = 0;
		int blockReportLength = 0;
		
		for (int i = 0; i < length; i++) {
			for (int r = 0; r < reportBitLength; r++) {
				System.out.println("w=" + reportBits[r]);
				if (reportBits[r] != iterator.nextDoc()) {
					throw new RuntimeException();
				}
			}
			reportBitLength = 0;
			for (int b = 0; b < blockReportLength; b++) {
				System.out.println("c="+bit);
				if (bit != iterator.nextDoc()) {
					throw new RuntimeException();
				}
				bit++;
			}
			blockReportLength = 0;
			if (compressedBits.fastGet(i)) {
				long bitCount = (words[i] & NOT_SET_MSB) << 6;
				if ((words[i] & SET_MSB) == 0) {
					//false block
					bit += bitCount;
				} else {
					//true block
					blockReportLength = (int) bitCount;
				}
			} else {
				long word = words[i];
				reportBitLength = 0;
				INNER:
				while (word != 0) {
					int ntz = BitUtil.ntz(word);
					bit += ntz;
					reportBits[reportBitLength++] = bit;
					bit++;
					if (ntz == 63) {
						break INNER;
					}
					word = word >>> (ntz + 1);
				}
				bit = ((bit - 1 >> 6l) + 1) << 6l;
			}
		}
	}
	
	public void debug() {
		int length = words.length;
		int bit = 0;
		int[] reportBits = new int[64];
		int reportBitLength = 0;
		int blockReportLength = 0;
		
		for (int i = 0; i < length; i++) {
			for (int r = 0; r < reportBitLength; r++) {
				System.out.println("w=" + reportBits[r]);
			}
			reportBitLength = 0;
			for (int b = 0; b < blockReportLength; b++) {
				System.out.println("c="+bit);
				bit++;
			}
			blockReportLength = 0;
			if (compressedBits.fastGet(i)) {
				long bitCount = (words[i] & NOT_SET_MSB) << 6;
				if ((words[i] & SET_MSB) == 0) {
					//false block
					bit += bitCount;
				} else {
					//true block
					blockReportLength = (int) bitCount;
				}
			} else {
				long word = words[i];
				reportBitLength = 0;
				while (word != 0) {
					int ntz = BitUtil.ntz(word);
					bit += ntz;
					reportBits[reportBitLength++] = bit;
					bit++;
					word = word >>> (ntz + 1);
				}
				bit = ((bit - 1 >> 6l) + 1) << 6l;
			}
		}
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
	public byte[] toBytes() {
		throw new RuntimeException();
	}

	@Override
	public CompressedBitSet toBytes(byte[] bytes) {
		throw new RuntimeException();
	}

	@Override
	public long getOriginalMemorySize() {
		// TODO Auto-generated method stub
		return 0;
	}
}
