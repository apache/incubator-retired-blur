package com.nearinfinity.lucene.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.OpenBitSet;

public class CompressedBitSetInt extends CompressedBitSet {

	private static final long serialVersionUID = 3034498789134318888L;
	private static final int WORD_SHIFT = 5;
	private static final int WORD_LENGTH_BYTES = 4;
	private static final int WORD_LENGTH_BITS = 32;
	
	private static final int ALL_ZEROS = 0; // 0x00000000l
	private static final int ALL_ONES = -1; // 0xFFFFFFFFl
	private static final int SET_MSB = Integer.MIN_VALUE; // 0x80000000l
	private static final int NOT_SET_MSB = 0x7FFFFFFF;
	private int[] words;
	private OpenBitSet compressedBits = new OpenBitSet();
	private long originalSize = 0;

	public CompressedBitSetInt(OpenBitSet bitSet) {
		compress(bitSet);
	}

	public CompressedBitSetInt(Iterator<Integer> bitsToSet) {
		compress(createBitSet(bitsToSet));
	}

	private OpenBitSet createBitSet(Iterator<Integer> bitsToSet) {
		OpenBitSet bitSet = new OpenBitSet();
		while (bitsToSet.hasNext()) {
			bitSet.set(bitsToSet.next());
		}
		return bitSet;
	}

	public long getMemorySize() {
		return (words.length + compressedBits.getBits().length) * WORD_LENGTH_BYTES;
	}
	
	public long getOriginalMemorySize() {
		return originalSize;
	}

	public boolean get(int index) {
		int neededLogicalCursorPosition = index >> WORD_SHIFT;
		int logicalCursor = 0;
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

	private boolean isMsbSet(int word) {
		if ((word & SET_MSB) != 0) {
			return true;
		}
		return false;
	}

	private void compress(OpenBitSet bitSet) {
		originalSize = bitSet.getBits().length * 8;
		int[] bits = convert(bitSet.getBits());
		checkNumberOfFilledWords(bits);
		int[] compBits = new int[bits.length * 2];
		int indexOfCompression = 0;
		for (int i = 0; i < bits.length; i++) {
			int l = bits[i];
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
		words = new int[indexOfCompression];
		System.arraycopy(compBits, 0, words, 0, indexOfCompression);
	}

	private int[] convert(long[] bits) {
		int[] d = new int[bits.length * 2];
		int intCount = 0;
		for (int i = 0; i < bits.length; i++) {
			d[intCount++] = (int) (bits[i] & 0xFFFFFFFF);
			d[intCount++] = (int) ((bits[i] >>> 32) & 0xFFFFFFFF);
		}
		return d;
	}

	private void checkNumberOfFilledWords(int[] bits) {
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
			private int[] reportBits = new int[WORD_LENGTH_BITS];
			private int reportBitLength = 0;
			private int blockReportLength = 0;
			private int wordCounter = 0;
			private int reportCounter = 0;
			private int blockCounter = 0;

			@Override
			public int nextDoc() throws IOException {
				for (; wordCounter < length; wordCounter++) {
					
					if (finishReportingLastBlocks() != -1) return doc;
					
					int word = words[wordCounter];
					
					if (compressedBits.get(wordCounter)) {
						int bitCount = (word & NOT_SET_MSB) << WORD_SHIFT;
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
							if (ntz == WORD_LENGTH_BITS - 1) break INNER;
							word = word >>> (ntz + 1);
						}
						//moves bit to beginning of next word
						bit = ((bit - 1 >> WORD_SHIFT) + 1) << WORD_SHIFT;
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
				for (; wordCounter < length; wordCounter++) {
					
					if (finishReportingLastBlocks(target) != -1) return doc;

					int word = words[wordCounter];
					
					if (compressedBits.get(wordCounter)) {
						int bitCount = (word & NOT_SET_MSB) << WORD_SHIFT;
						if ((word & SET_MSB) == 0) {//false block
							bit += bitCount;
						} else {//true block
							blockReportLength = (int) bitCount;
						}
					} else {
						if (bit + WORD_LENGTH_BITS >= target) {
							INNER:
							while (word != 0) {
								int ntz = BitUtil.ntz(word);
								bit += ntz;
								reportBits[reportBitLength++] = bit;
								bit++;
								if (ntz == WORD_LENGTH_BITS - 1) break INNER;
								word = word >>> (ntz + 1);
							}
							//moves bit to beginning of next word
							bit = ((bit - 1 >> WORD_SHIFT) + 1) << WORD_SHIFT;
						} else {
							bit += WORD_LENGTH_BITS;
						}
					}
				}
				
				if (finishReportingLastBlocks(target) != -1) return doc;
				
				return noMoreDocs();
			}

			private int finishReportingLastBlocks(int target) {
				while (reportCounter < reportBitLength) {
					int ndoc = reportBits[reportCounter++];
					if (ndoc >= target) {
						return doc = ndoc;
					}
				}
				reportCounter = 0;
				reportBitLength = 0;
				
				if (blockReportLength + bit < target) {
					bit += blockReportLength;
				} else {
					while (blockCounter < blockReportLength) {
						doc = bit;
						bit++;
						blockCounter++;
						if (doc >= target) {
							return doc;
						}
					}
				}
				blockCounter = 0;
				blockReportLength = 0;
				return -1;
			}

			private int noMoreDocs() {
				return doc = NO_MORE_DOCS;
			}
		};
	}

	
	public void debug(DocIdSetIterator iterator) throws IOException {
		int length = words.length;
		int bit = 0;
		int[] reportBits = new int[WORD_LENGTH_BITS];
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
				int bitCount = (words[i] & NOT_SET_MSB) << WORD_SHIFT;
				if ((words[i] & SET_MSB) == 0) {
					//false block
					bit += bitCount;
				} else {
					//true block
					blockReportLength = (int) bitCount;
				}
			} else {
				int word = words[i];
				reportBitLength = 0;
				INNER:
				while (word != 0) {
					int ntz = BitUtil.ntz(word);
					bit += ntz;
					reportBits[reportBitLength++] = bit;
					bit++;
					if (ntz == WORD_LENGTH_BITS - 1) {
						break INNER;
					}
					word = word >>> (ntz + 1);
				}
				bit = ((bit - 1 >> WORD_SHIFT) + 1) << WORD_SHIFT;
			}
		}
	}
	
	public void debug() {
		int length = words.length;
		int bit = 0;
		int[] reportBits = new int[WORD_LENGTH_BITS];
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
				int bitCount = (words[i] & NOT_SET_MSB) << WORD_SHIFT;
				if ((words[i] & SET_MSB) == 0) {
					//false block
					bit += bitCount;
				} else {
					//true block
					blockReportLength = (int) bitCount;
				}
			} else {
				int word = words[i];
				reportBitLength = 0;
				while (word != 0) {
					int ntz = BitUtil.ntz(word);
					bit += ntz;
					reportBits[reportBitLength++] = bit;
					bit++;
					word = word >>> (ntz + 1);
				}
				bit = ((bit - 1 >> WORD_SHIFT) + 1) << WORD_SHIFT;
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		OpenBitSet bitSet = new OpenBitSet();
		bitSet.set(1);
		bitSet.set(1000);
		System.out.println(new CompressedBitSetInt(bitSet).iterator().advance(80));
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
	public byte[] toBytes() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream outputStream = new ObjectOutputStream(baos);
		outputStream.writeObject(this);
		outputStream.close();
		return baos.toByteArray();
	}

	@Override
	public CompressedBitSet toBytes(byte[] bytes) throws IOException {
		ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
		try {
			return (CompressedBitSet) inputStream.readObject();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} finally {
			inputStream.close();
		}
	}
}
