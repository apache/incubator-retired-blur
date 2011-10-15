package com.nearinfinity.blur.store.blockcache;

import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.OpenBitSet;

public class BlockLocks {

  private AtomicLongArray bits;
  private int wlen;

  public BlockLocks(long numBits) {
    int length = OpenBitSet.bits2words(numBits);
    bits = new AtomicLongArray(length);
    wlen = length;
  }

  /**
   * Find the next clear bit in the bit set.
   * @param index
   * @return
   */
  public int nextClearBit(int index) {
    int i = index >> 6;
    if (i >= wlen)
      return -1;
    int subIndex = index & 0x3f; // index within the word
    long word = ~bits.get(i) >> subIndex; // skip all the bits to the right of
                                          // index
    if (word != 0) {
      return (i << 6) + subIndex + BitUtil.ntz(word);
    }
    while (++i < wlen) {
      word = ~bits.get(i);
      if (word != 0) {
        return (i << 6) + BitUtil.ntz(word);
      }
    }
    return -1;
  }

  /**
   * Thread safe set operation that will set the bit if and only if the bit was not previously set.
   * @param index the index position to set.
   * @return returns true if the bit was set and false if it was already set.
   */
  public boolean set(int index) {
    int wordNum = index >> 6; // div 64
    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    long word, oword;
    do {
      word = bits.get(wordNum);
      // if set another thread stole the lock
      if ((word & bitmask) != 0) {
        return false;
      }
      oword = word;
      word |= bitmask;
    } while (bits.compareAndSet(wordNum, oword, word));
    return true;
  }

  public void clear(int index) {
    int wordNum = index >> 6;
    int bit = index & 0x03f;
    long bitmask = 1L << bit;
    long word, oword;
    do {
      word = bits.get(wordNum);
      oword = word;
      word &= ~bitmask;
    } while (bits.compareAndSet(wordNum, oword, word));
  }
}
