/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.store.blockcache_v2;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.store.blockcache.BlockLocks;
import org.apache.blur.store.blockcache_v2.BaseCache.STORE;
import org.apache.blur.store.blockcache_v2.cachevalue.UnsafeWrappedCacheValue;
import org.apache.blur.store.blockcache_v2.cachevalue.UnsafeWrapperMultiCacheValue;
import org.apache.blur.store.util.UnsafeUtil;

import sun.misc.Unsafe;

public class SlabAllocationCacheValueBufferPool extends BaseCacheValueBufferPool {

  private static final Unsafe _unsafe;

  private final Collection<Slab> _slabs;
  private final int _chunkSize;
  private final int _slabSize;
  private final int _numberOfBlocksPerSlab;

  static {
    _unsafe = UnsafeUtil.getUnsafe();
  }

  public SlabAllocationCacheValueBufferPool(int chunkSize, int slabSize) {
    super(STORE.OFF_HEAP);
    _chunkSize = chunkSize;
    _slabSize = slabSize;
    _numberOfBlocksPerSlab = _slabSize / _chunkSize;
    _slabs = Collections.newSetFromMap(new ConcurrentHashMap<Slab, Boolean>());
  }

  static class Slab {
    final long _address;
    final BlockLocks _locks;
    final int _chunkSize;
    final long _maxAddress;

    Slab(long address, int numberOfChunks, int chunkSize) {
      _address = address;
      _maxAddress = _address + ((long) numberOfChunks * (long) chunkSize);
      _locks = new BlockLocks(numberOfChunks);
      _chunkSize = chunkSize;
    }

    long findChunk() {
      while (true) {
        int chunkId = _locks.nextClearBit(0);
        if (chunkId < 0) {
          return -1L;
        }
        if (_locks.set(chunkId)) {
          return _address + ((long) chunkId * (long) _chunkSize);
        }
      }
    }

    boolean releaseIfValid(long address) {
      if (address >= _address && address < _maxAddress) {
        long offset = address - _address;
        int index = (int) (offset / _chunkSize);
        _locks.clear(index);
      }
      return false;
    }

    void release() {
      _unsafe.freeMemory(_address);
    }
  }

  @Override
  public CacheValue getCacheValue(int cacheBlockSize) {
    validCacheBlockSize(cacheBlockSize);
    int numberOfChunks = getNumberOfChunks(cacheBlockSize);
    if (numberOfChunks == 1) {
      while (true) {
        Collection<Slab> slabs = getSlabs();
        for (Slab slab : slabs) {
          final long chunkAddress = slab.findChunk();
          if (chunkAddress >= 0) {
            // found one!
            return new UnsafeWrappedCacheValue(cacheBlockSize, chunkAddress) {
              @Override
              protected void releaseInternal() {
                releaseChunk(chunkAddress);
              }
            };
          }
        }
        maybeAllocateNewSlab(slabs.size());
      }
    } else {
      final long[] chunkAddresses = new long[numberOfChunks];
      int chunksFound = 0;
      while (true) {
        Collection<Slab> slabs = getSlabs();
        for (Slab slab : slabs) {
          INNER: while (chunksFound < numberOfChunks) {
            long chunkAddress = slab.findChunk();
            if (chunkAddress >= 0) {
              // found one!
              chunkAddresses[chunksFound] = chunkAddress;
              chunksFound++;
            } else {
              break INNER;
            }
          }
        }
        if (chunksFound == numberOfChunks) {
          return new UnsafeWrapperMultiCacheValue(cacheBlockSize, chunkAddresses, _chunkSize) {
            @Override
            protected void releaseInternal() {
              releaseChunks(chunkAddresses);
            }
          };
        }
        maybeAllocateNewSlab(slabs.size());
      }
    }

  }

  private synchronized void maybeAllocateNewSlab(int numberOfSlabs) {
    Collection<Slab> slabs = getSlabs();
    if (slabs.size() == numberOfSlabs) {
      allocateNewSlab();
    }
    return;
  }

  private void allocateNewSlab() {
    long address = _unsafe.allocateMemory(_slabSize);
    _slabs.add(new Slab(address, _numberOfBlocksPerSlab, _chunkSize));
  }

  private Collection<Slab> getSlabs() {
    return _slabs;
  }

  private void releaseChunks(long[] addresses) {
    for (long address : addresses) {
      releaseChunk(address);
    }
  }

  private void releaseChunk(long address) {
    Collection<Slab> slabs = getSlabs();
    for (Slab slab : slabs) {
      if (slab.releaseIfValid(address)) {
        return;
      }
    }
  }

  @Override
  public void returnToPool(CacheValue cacheValue) {
    cacheValue.release();
  }

  @Override
  public void close() throws IOException {
    Collection<Slab> slabs = getSlabs();
    for (Slab slab : slabs) {
      slab.release();
    }
  }

  private void validCacheBlockSize(int cacheBlockSize) {
    if (cacheBlockSize >= 1) {
      return;
    }
    throw new RuntimeException("CacheBlockSize requested [" + cacheBlockSize + "] is invalid.");
  }

  private int getNumberOfChunks(int cacheBlockSize) {
    if (cacheBlockSize <= _chunkSize) {
      return 1;
    }
    int chunks = cacheBlockSize / _chunkSize;
    if (cacheBlockSize % _chunkSize == 0) {
      return chunks;
    }
    return chunks + 1;
  }
}
