package com.nearinfinity.blur.store.blockcache;

import java.util.BitSet;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.map.LRUMap;

public class BlockCache {

  public int getSize() {
    return _cache.size();
  }

  private Map<BlockCacheKey, BlockCacheLocation> _cache;
  private byte[][] _banks;
  private BitSet[] _bitSets;
  private AtomicInteger[] _bitSetCounters;
  private int _blockSize;
  private int _numberOfBlocksPerBank;
  private int _maxEntries;

  @SuppressWarnings("unchecked")
  public BlockCache(final int numberOfBanks, final int numberOfBlocksPerBank,
      int blockSize) {
    _numberOfBlocksPerBank = numberOfBlocksPerBank;
    _banks = new byte[numberOfBanks][];
    _bitSets = new BitSet[numberOfBanks];
    _bitSetCounters = new AtomicInteger[numberOfBanks];
    _maxEntries = (numberOfBlocksPerBank * numberOfBanks) - 1;
    for (int i = 0; i < numberOfBanks; i++) {
      _banks[i] = new byte[numberOfBlocksPerBank * blockSize];
      _bitSets[i] = new BitSet(numberOfBlocksPerBank);
      _bitSetCounters[i] = new AtomicInteger();
    }
    _cache = Collections.synchronizedMap(new LRUMap(_maxEntries) {
      private static final long serialVersionUID = 2091289339926232984L;
      @Override
      protected boolean removeLRU(LinkEntry entry) {
        BlockCacheLocation location = (BlockCacheLocation) entry.getValue();
        int bankId = location.getBankId();
        int block = location.getBlock();
        _bitSets[bankId].clear(block);
        _bitSetCounters[bankId].decrementAndGet();
        // double seconds = (System.currentTimeMillis() -
        // location.getLastAccess()) / 1000.0;
        // System.out.println("Last Accessed [" + seconds + "] ago with [" +
        // location.getNumberOfAccesses() + "] accesses.");
        return true;
      }
    });
    _blockSize = blockSize;
  }

  public boolean store(BlockCacheKey blockCacheKey, byte[] data) {
    checkLength(data);
    BlockCacheLocation location = _cache.get(blockCacheKey);
    boolean newLocation = false;
    if (location == null) {
      newLocation = true;
      location = new BlockCacheLocation();
      if (!findEmptyLocation(location)) {
        return false;
      }
    }
    int bankId = location.getBankId();
    int offset = location.getBlock() * _blockSize;
    byte[] bank = getBank(bankId);
    System.arraycopy(data, 0, bank, offset, _blockSize);
    if (newLocation) {
      _cache.put(blockCacheKey.clone(), location);
    }
    return true;
  }

  public boolean fetch(BlockCacheKey blockCacheKey, byte[] buffer, int blockOffset, int off, int length) {
    BlockCacheLocation location = _cache.get(blockCacheKey);
    if (location == null) {
      return false;
    }
    int bankId = location.getBankId();
    int offset = location.getBlock() * _blockSize;
    location.touch();
    byte[] bank = getBank(bankId);
    System.arraycopy(bank, offset + blockOffset, buffer, off, length);
    return true;
  }

  public boolean fetch(BlockCacheKey blockCacheKey, byte[] buffer) {
    checkLength(buffer);
    BlockCacheLocation location = _cache.get(blockCacheKey);
    if (location == null) {
      return false;
    }
    int bankId = location.getBankId();
    int offset = location.getBlock() * _blockSize;
    location.touch();
    byte[] bank = getBank(bankId);
    System.arraycopy(bank, offset, buffer, 0, _blockSize);
    return true;
  }

  private boolean findEmptyLocation(BlockCacheLocation location) {
    for (int bankId = 0; bankId < _banks.length; bankId++) {
      BitSet bitSet = _bitSets[bankId];
      if (isFullBitSet(bankId)) {
        continue;
      }
      int bit = bitSet.nextClearBit(0);
      if (bit >= _numberOfBlocksPerBank) {
        continue;
      }
      if (bit != -1) {
        location.setBankId(bankId);
        location.setBlock(bit);
        bitSet.set(bit, true);
        _bitSetCounters[bankId].incrementAndGet();
        return true;
      }
    }
    return false;
  }

  private boolean isFullBitSet(int bank) {
    if (_bitSetCounters[bank].get() == _numberOfBlocksPerBank) {
      return true;
    }
    return false;
  }

  private void checkLength(byte[] buffer) {
    if (buffer.length != _blockSize) {
      throw new RuntimeException("Buffer wrong size, expecting [" + _blockSize
          + "] got [" + buffer.length + "]");
    }
  }

  private byte[] getBank(int bankId) {
    return _banks[bankId];
  }

}
