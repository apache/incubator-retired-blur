package com.nearinfinity.blur.store.blockcache;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.map.LRUMap;

public class BlockCache {

  private final Map<BlockCacheKey, BlockCacheLocation> _cache;
  private final byte[][] _banks;
  private final BlockLocks[] _locks;
  private final AtomicInteger[] _lockCounters;
  private final int _blockSize;
  private final int _numberOfBlocksPerBank;
  private final int _maxEntries;

  @SuppressWarnings("unchecked")
  public BlockCache(final int numberOfBanks, final int numberOfBlocksPerBank, int blockSize) {
    _numberOfBlocksPerBank = numberOfBlocksPerBank;
    _banks = new byte[numberOfBanks][];
    _locks = new BlockLocks[numberOfBanks];
    _lockCounters = new AtomicInteger[numberOfBanks];
    _maxEntries = (numberOfBlocksPerBank * numberOfBanks) - 1;
    for (int i = 0; i < numberOfBanks; i++) {
      _banks[i] = new byte[numberOfBlocksPerBank * blockSize];
      _locks[i] = new BlockLocks(numberOfBlocksPerBank);
      _lockCounters[i] = new AtomicInteger();
    }
    _cache = Collections.synchronizedMap(new LRUMap(_maxEntries) {
      private static final long serialVersionUID = 2091289339926232984L;
      @Override
      protected boolean removeLRU(LinkEntry entry) {
        BlockCacheLocation location = (BlockCacheLocation) entry.getValue();
        synchronized (location) {
          int bankId = location.getBankId();
          int block = location.getBlock();
          location.setRemoved(true);
          _locks[bankId].clear(block);
          _lockCounters[bankId].decrementAndGet();
          return true;
        }
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
    synchronized (location) {
      if (location.isRemoved()) {
        return false;
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
  }

  public boolean fetch(BlockCacheKey blockCacheKey, byte[] buffer, int blockOffset, int off, int length) {
    BlockCacheLocation location = _cache.get(blockCacheKey);
    if (location == null) {
      return false;
    }
    synchronized (location) {
      if (location.isRemoved()) {
        return false;
      }
      int bankId = location.getBankId();
      int offset = location.getBlock() * _blockSize;
      location.touch();
      byte[] bank = getBank(bankId);
      System.arraycopy(bank, offset + blockOffset, buffer, off, length);
      return true;
    }
  }

  public boolean fetch(BlockCacheKey blockCacheKey, byte[] buffer) {
    checkLength(buffer);
    return fetch(blockCacheKey,buffer,0,0,_blockSize);
  }

  private boolean findEmptyLocation(BlockCacheLocation location) {
    OUTER:
    for (int bankId = 0; bankId < _banks.length; bankId++) {
      AtomicInteger bitSetCounter = _lockCounters[bankId];
      BlockLocks bitSet = _locks[bankId];
      if (bitSetCounter.get() == _numberOfBlocksPerBank) {
        //if bitset is full
        continue OUTER;
      }
      //this needs to spin, if a lock was attempted but not obtained the rest of the bank is skipped
      int bit = bitSet.nextClearBit(0);
      INNER:
      while (bit != -1) {
        if (bit >= _numberOfBlocksPerBank) {
          //bit set is full
          continue OUTER;
        }
        if (!bitSet.set(bit)) {
          //lock was not obtained
          //this restarts at 0 because another block could have been unlocked while this was executing
          bit = bitSet.nextClearBit(0);
          continue INNER;
        } else {
          //lock obtained
          location.setBankId(bankId);
          location.setBlock(bit);
          bitSetCounter.incrementAndGet();
          return true;
        }
      }
    }
    return false;
  } 

  private void checkLength(byte[] buffer) {
    if (buffer.length != _blockSize) {
      throw new RuntimeException("Buffer wrong size, expecting [" + _blockSize + "] got [" + buffer.length + "]");
    }
  }

  private byte[] getBank(int bankId) {
    return _banks[bankId];
  }

  public int getSize() {
    return _cache.size();
  }
}
