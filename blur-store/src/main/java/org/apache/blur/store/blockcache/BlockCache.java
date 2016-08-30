package org.apache.blur.store.blockcache;

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
import static org.apache.blur.metrics.MetricsConstants.CACHE;
import static org.apache.blur.metrics.MetricsConstants.ENTRIES;
import static org.apache.blur.metrics.MetricsConstants.EVICTION;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.SIZE;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.blur.utils.ThreadValue;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class BlockCache implements Closeable {

  /**
   * <code>true</code>, if this platform supports unmapping mmapped files.
   */
  public static final boolean UNMAP_SUPPORTED;
  static {
    boolean v;
    try {
      Class.forName("sun.misc.Cleaner");
      Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
      v = true;
    } catch (Exception e) {
      v = false;
    }
    UNMAP_SUPPORTED = v;
  }

  public static final int _128M = 134217728;
  public static final int _8K = 8192;

  private final ConcurrentMap<BlockCacheKey, BlockCacheLocation> _cache;
  private final ByteBuffer[] _slabs;
  private final BlockLocks[] _locks;
  private final AtomicInteger[] _lockCounters;
  private final int _blockSize = _8K;
  private final int _numberOfBlocksPerSlab;
  private final int _maxEntries;
  private final Meter evictions;
  private final int _numberOfSlabs;
  private final boolean _directAllocation;
  private final ThreadValue<ByteBuffer[]> _threadLocalSlabs = new ThreadValue<ByteBuffer[]>() {
    @Override
    protected ByteBuffer[] initialValue() {
      return new ByteBuffer[_numberOfSlabs];
    }
  };
  
  //This turns the lazy bytebuffer allocation on or off.
  private final boolean lazy = true;

  public BlockCache(boolean directAllocation, long totalMemory) {
    this(directAllocation, totalMemory, _128M);
  }

  public BlockCache(boolean directAllocation, long totalMemory, int slabSize) {
    _numberOfBlocksPerSlab = slabSize / _blockSize;
    _numberOfSlabs = (int) (totalMemory / slabSize);
    _directAllocation = directAllocation;

    _slabs = new ByteBuffer[_numberOfSlabs];
    _locks = new BlockLocks[_numberOfSlabs];
    _lockCounters = new AtomicInteger[_numberOfSlabs];
    _maxEntries = (_numberOfBlocksPerSlab * _numberOfSlabs) - 1;
    for (int i = 0; i < _numberOfSlabs; i++) {
      if (!lazy) {
        if (_directAllocation) {
          _slabs[i] = ByteBuffer.allocateDirect(_numberOfBlocksPerSlab * _blockSize);
        } else {
          _slabs[i] = ByteBuffer.allocate(_numberOfBlocksPerSlab * _blockSize);
        }
      }
      _locks[i] = new BlockLocks(_numberOfBlocksPerSlab);
      _lockCounters[i] = new AtomicInteger();
    }

    evictions = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE, EVICTION), EVICTION, TimeUnit.SECONDS);
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, CACHE, ENTRIES), new Gauge<Long>() {
      @Override
      public Long value() {
        return (long) getSize();
      }
    });
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, CACHE, SIZE), new Gauge<Long>() {
      @Override
      public Long value() {
        return ((long) getSize()) * (long) _8K;
      }
    });

    EvictionListener<BlockCacheKey, BlockCacheLocation> listener = new EvictionListener<BlockCacheKey, BlockCacheLocation>() {
      @Override
      public void onEviction(BlockCacheKey key, BlockCacheLocation location) {
        releaseLocation(location);
        evictions.mark();
      }
    };
    _cache = new ConcurrentLinkedHashMap.Builder<BlockCacheKey, BlockCacheLocation>()
        .maximumWeightedCapacity(_maxEntries).listener(listener).build();
  }

  private void releaseLocation(BlockCacheLocation location) {
    if (location == null) {
      return;
    }
    int slabId = location.getSlabId();
    int block = location.getBlock();
    location.setRemoved(true);
    _locks[slabId].clear(block);
    _lockCounters[slabId].decrementAndGet();
  }

  public boolean store(BlockCacheKey blockCacheKey, int blockOffset, byte[] data, int offset, int length) {
    if (length + blockOffset > _blockSize) {
      throw new RuntimeException("Buffer size exceeded, expecting max [" + _blockSize + "] got length [" + length
          + "] with blockOffset [" + blockOffset + "]");
    }
    BlockCacheLocation location = _cache.get(blockCacheKey);
    boolean newLocation = false;
    if (location == null) {
      newLocation = true;
      location = new BlockCacheLocation();
      if (!findEmptyLocation(location)) {
        return false;
      }
    }
    if (location.isRemoved()) {
      return false;
    }
    int slabId = location.getSlabId();
    int slabOffset = location.getBlock() * _blockSize;
    ByteBuffer slab = getSlab(slabId);
    slab.position(slabOffset + blockOffset);
    slab.put(data, offset, length);
    if (newLocation) {
      releaseLocation(_cache.put(blockCacheKey.clone(), location));
    }
    return true;
  }

  public boolean fetch(BlockCacheKey blockCacheKey, byte[] buffer, int blockOffset, int off, int length) {
    BlockCacheLocation location = _cache.get(blockCacheKey);
    if (location == null) {
      return false;
    }
    if (location.isRemoved()) {
      return false;
    }
    int slabId = location.getSlabId();
    int offset = location.getBlock() * _blockSize;
    location.touch();
    ByteBuffer slab = getSlab(slabId);
    slab.position(offset + blockOffset);
    slab.get(buffer, off, length);
    return true;
  }

  public boolean fetch(BlockCacheKey blockCacheKey, byte[] buffer) {
    checkLength(buffer);
    return fetch(blockCacheKey, buffer, 0, 0, _blockSize);
  }

  private boolean findEmptyLocation(BlockCacheLocation location) {
    // This is a tight loop that will try and find a location to
    // place the block before giving up
    for (int j = 0; j < 10; j++) {
      OUTER: for (int slabId = 0; slabId < _slabs.length; slabId++) {
        AtomicInteger bitSetCounter = _lockCounters[slabId];
        BlockLocks bitSet = _locks[slabId];
        if (bitSetCounter.get() == _numberOfBlocksPerSlab) {
          // if bitset is full
          continue OUTER;
        }
        // this check needs to spin, if a lock was attempted but not obtained
        // the rest of the slab should not be skipped
        int bit = bitSet.nextClearBit(0);
        INNER: while (bit != -1) {
          if (bit >= _numberOfBlocksPerSlab) {
            // bit set is full
            continue OUTER;
          }
          if (!bitSet.set(bit)) {
            // lock was not obtained
            // this restarts at 0 because another block could have been unlocked
            // while this was executing
            bit = bitSet.nextClearBit(0);
            continue INNER;
          } else {
            // lock obtained
            location.setSlabId(slabId);
            location.setBlock(bit);
            bitSetCounter.incrementAndGet();
            return true;
          }
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

  private ByteBuffer getSlab(int slabId) {
    if (!lazy) {
      return _slabs[slabId].duplicate();
    } else {
      ByteBuffer[] byteBuffers = _threadLocalSlabs.get();
      ByteBuffer byteBuffer = byteBuffers[slabId];
      if (byteBuffer == null) {
        synchronized (_slabs) {
          ByteBuffer bb = _slabs[slabId];
          if (bb == null) {
            if (_directAllocation) {
              bb = ByteBuffer.allocateDirect(_numberOfBlocksPerSlab * _blockSize);
            } else {
              bb = ByteBuffer.allocate(_numberOfBlocksPerSlab * _blockSize);
            }
            _slabs[slabId] = bb;
          }
          byteBuffer = bb.duplicate();
        }
        byteBuffers[slabId] = byteBuffer;
      }
      return byteBuffer;
    }
  }

  public int getSize() {
    if (_cache == null) {
      return 0;
    }
    return _cache.size();
  }

  public void close() throws IOException {
    for (ByteBuffer buffer : this._slabs) {
      freeBuffer(buffer);
    }
  }

  /**
   * This code was copied form MMAPDirectory in Lucene.
   */
  protected void freeBuffer(final ByteBuffer buffer) throws IOException {
    if (buffer == null) {
      return;
    }
    if (UNMAP_SUPPORTED) {
      try {
        AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            final Method getCleanerMethod = buffer.getClass().getMethod("cleaner");
            getCleanerMethod.setAccessible(true);
            final Object cleaner = getCleanerMethod.invoke(buffer);
            if (cleaner != null) {
              cleaner.getClass().getMethod("clean").invoke(cleaner);
            }
            return null;
          }
        });
      } catch (PrivilegedActionException e) {
        final IOException ioe = new IOException("unable to unmap the mapped buffer");
        ioe.initCause(e.getCause());
        throw ioe;
      }
    }
  }
}
