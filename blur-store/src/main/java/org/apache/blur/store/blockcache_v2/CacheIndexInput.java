/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.EOFException;
import java.io.IOException;

import org.apache.blur.store.blockcache_v2.cachevalue.ByteArrayCacheValue;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.store.buffer.Store;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;

public class CacheIndexInput extends IndexInput {

  private final long _fileLength;
  private final long _fileId;
  private final int _cacheBlockSize;
  private final int _bufferSize;
  private final CacheDirectory _directory;
  private final String _fileName;
  private final Cache _cache;
  private final Store _store;
  private final IndexInputCache _indexInputCache;

  private IndexInput _indexInput;
  private CacheKey _key = new CacheKey();
  private CacheValue _cacheValue;
  private CacheValue _cacheValueQuietRefCannotBeReleased;

  private long _position;
  private int _blockPosition;
  private boolean _quiet;
  private boolean _isClosed;

  public CacheIndexInput(CacheDirectory directory, String fileName, IndexInput indexInput, Cache cache)
      throws IOException {
    super("CacheIndexInput(" + indexInput.toString() + ")");
    _directory = directory;
    _fileName = fileName;
    _indexInput = indexInput;
    _fileLength = indexInput.length();
    _cache = cache;

    _fileId = _cache.getFileId(_directory, _fileName);
    _cacheBlockSize = _cache.getCacheBlockSize(_directory, _fileName);
    _indexInputCache = _cache.createIndexInputCache(_directory, _fileName, _fileLength);
    _bufferSize = _cache.getFileBufferSize(_directory, _fileName);
    _quiet = _cache.shouldBeQuiet(_directory, _fileName);
    _key.setFileId(_fileId);
    _isClosed = false;
    _store = BufferStore.instance(_bufferSize);
  }

  @Override
  public int readVInt() throws IOException {
    if (isCacheValueValid() && remaining() >= 5) {
      byte b;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      if (b >= 0)
        return b;
      int i = b & 0x7F;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      i |= (b & 0x7F) << 7;
      if (b >= 0)
        return i;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      i |= (b & 0x7F) << 14;
      if (b >= 0)
        return i;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      i |= (b & 0x7F) << 21;
      if (b >= 0)
        return i;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
      i |= (b & 0x0F) << 28;
      if ((b & 0xF0) == 0)
        return i;
      throw new IOException("Invalid vInt detected (too many bits)");
    }
    return super.readVInt();
  }

  private boolean isCacheValueValid() {
    if (_cacheValue != null && !_cacheValue.isEvicted()) {
      return true;
    }
    return false;
  }

  @Override
  public long readVLong() throws IOException {
    if (isCacheValueValid() && remaining() >= 9) {
      byte b;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      if (b >= 0)
        return b;
      long i = b & 0x7FL;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      i |= (b & 0x7FL) << 7;
      if (b >= 0)
        return i;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      i |= (b & 0x7FL) << 14;
      if (b >= 0)
        return i;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      i |= (b & 0x7FL) << 21;
      if (b >= 0)
        return i;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      i |= (b & 0x7FL) << 28;
      if (b >= 0)
        return i;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      i |= (b & 0x7FL) << 35;
      if (b >= 0)
        return i;
      try {
        b = readByteFromCache();
      } catch (EvictionException e2) {
        b = readByte();
      }
      i |= (b & 0x7FL) << 42;
      if (b >= 0)
        return i;
      try {
        b = readByteFromCache();
      } catch (EvictionException e1) {
        b = readByte();
      }
      i |= (b & 0x7FL) << 49;
      if (b >= 0)
        return i;
      try {
        b = readByteFromCache();
      } catch (EvictionException e) {
        b = readByte();
      }
      i |= (b & 0x7FL) << 56;
      if (b >= 0)
        return i;
      throw new IOException("Invalid vLong detected (negative values disallowed)");
    }
    return super.readVLong();
  }

  @Override
  public byte readByte() throws IOException {
    ensureOpen();
    byte b;
    tryToFill();
    try {
      b = _cacheValue.read(_blockPosition);
    } catch (EvictionException e) {
      releaseCache();
      return readByte();
    }
    _position++;
    _blockPosition++;
    return b;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    ensureOpen();
    while (len > 0) {
      tryToFill();
      int remaining = remaining();
      int length = Math.min(len, remaining);
      try {
        _cacheValue.read(_blockPosition, b, offset, length);
      } catch (EvictionException e) {
        releaseCache();
        readBytes(b, offset, len);
        return;
      }
      offset += length;
      len -= length;
      _position += length;
      _blockPosition += length;
    }
  }

  @Override
  public short readShort() throws IOException {
    ensureOpen();
    if (isCacheValueValid() && remaining() >= 2) {
      short s;
      try {
        s = _cacheValue.readShort(_blockPosition);
      } catch (EvictionException e) {
        return super.readShort();
      }
      _blockPosition += 2;
      _position += 2;
      return s;
    }
    return super.readShort();
  }

  @Override
  public int readInt() throws IOException {
    ensureOpen();
    if (isCacheValueValid() && remaining() >= 4) {
      int i;
      try {
        i = _cacheValue.readInt(_blockPosition);
      } catch (EvictionException e) {
        return super.readInt();
      }
      _blockPosition += 4;
      _position += 4;
      return i;
    }
    return super.readInt();
  }

  @Override
  public long readLong() throws IOException {
    ensureOpen();
    if (isCacheValueValid() && remaining() >= 8) {
      long l;
      try {
        l = _cacheValue.readLong(_blockPosition);
      } catch (EvictionException e) {
        return super.readLong();
      }
      _blockPosition += 8;
      _position += 8;
      return l;
    }
    return super.readLong();
  }

  @Override
  public void close() throws IOException {
    if (!_isClosed) {
      _isClosed = true;
      _indexInput.close();
      releaseCache();
      _indexInputCache.close();
    }
  }

  @Override
  public long getFilePointer() {
    ensureOpen();
    return _position;
  }

  private void checkEOF() throws EOFException {
    if (_position >= _fileLength) {
      throw new EOFException("read past EOF: " + this.toString());
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    ensureOpen();
    if (pos >= _fileLength) {
      _position = pos;
      releaseCache();
      return;
    }
    if (_position == pos) {
      // Seeking to same position
      return;
    }
    long oldBlockId = getBlockId();
    if (_blockPosition == _cacheBlockSize) {
      // If we are at the end of the current block, but haven't actually fetched
      // the next block then we are really on the previous.
      oldBlockId--;
    }
    _position = pos;
    long newBlockId = getBlockId(_position);
    if (newBlockId == oldBlockId) {
      // need to set new block position
      _blockPosition = getBlockPosition();
    } else {
      releaseCache();
    }
  }

  @Override
  public long length() {
    ensureOpen();
    return _fileLength;
  }

  @Override
  public IndexInput clone() {
    ensureOpen();
    CacheIndexInput clone = (CacheIndexInput) super.clone();
    clone._key = _key.clone();
    clone._indexInput = _indexInput.clone();
    clone._quiet = _cache.shouldBeQuiet(_directory, _fileName);
    clone._cacheValueQuietRefCannotBeReleased = null;
    return clone;
  }

  private byte readByteFromCache() throws EvictionException {
    byte b = _cacheValue.read(_blockPosition);
    _position++;
    _blockPosition++;
    return b;
  }

  private int remaining() {
    try {
      return _cacheValue.length() - _blockPosition;
    } catch (EvictionException e) {
      return 0;
    }
  }

  private void tryToFill() throws IOException {
    checkEOF();
    if (!isCacheValueValid() || remaining() == 0) {
      releaseCache();
      fill();
    } else {
      return;
    }
  }

  private void releaseCache() {
    if (_cacheValue != null) {
      _cacheValue = null;
    }
  }

  private void fillQuietly() throws IOException {
    _key.setBlockId(getBlockId());
    _cacheValue = lookup(true);
    if (_cacheValue == null) {
      if (_cacheValueQuietRefCannotBeReleased == null) {
        // @TODO this could be improved.
        int cacheBlockSize = _cache.getCacheBlockSize(_directory, _fileName);
        _cacheValueQuietRefCannotBeReleased = new ByteArrayCacheValue(cacheBlockSize);
      }
      _cacheValue = _cacheValueQuietRefCannotBeReleased;
      long filePosition = getFilePosition();
      _indexInput.seek(filePosition);
      byte[] buffer = _store.takeBuffer(_bufferSize);
      int len = (int) Math.min(_cacheBlockSize, _fileLength - filePosition);
      int cachePosition = 0;
      while (len > 0) {
        int length = Math.min(_bufferSize, len);
        _indexInput.readBytes(buffer, 0, length);
        _cacheValue.write(cachePosition, buffer, 0, length);
        len -= length;
        cachePosition += length;
      }
      _store.putBuffer(buffer);
    }
    _blockPosition = getBlockPosition();
  }

  private void fillNormally() throws IOException {
    _key.setBlockId(getBlockId());
    _cacheValue = lookup(false);
    if (_cacheValue == null) {
      _cacheValue = _cache.newInstance(_directory, _fileName);
      long filePosition = getFilePosition();
      _indexInput.seek(filePosition);
      byte[] buffer = _store.takeBuffer(_bufferSize);
      int len = (int) Math.min(_cacheBlockSize, _fileLength - filePosition);
      int cachePosition = 0;
      while (len > 0) {
        int length = Math.min(_bufferSize, len);
        _indexInput.readBytes(buffer, 0, length);
        _cacheValue.write(cachePosition, buffer, 0, length);
        len -= length;
        cachePosition += length;
      }
      _store.putBuffer(buffer);
      _cache.put(_directory, _fileName, _key.clone(), _cacheValue);
    }
    _blockPosition = getBlockPosition();
  }

  private CacheValue lookup(boolean quietly) {
    CacheValue cacheValue = _indexInputCache.get(_key.getBlockId());
    if (cacheValue == null) {
      if (quietly) {
        cacheValue = _cache.getQuietly(_directory, _fileName, _key);
      } else {
        cacheValue = _cache.get(_directory, _fileName, _key);
      }
    }
    if (cacheValue != null) {
      _indexInputCache.put(_key.getBlockId(), cacheValue);
    }
    return cacheValue;
  }

  private void fill() throws IOException {
    if (_quiet) {
      fillQuietly();
    } else {
      fillNormally();
    }
  }

  private int getBlockPosition() {
    return (int) (_position % _cacheBlockSize);
  }

  private long getFilePosition() {
    // make this a mask...?
    return getBlockId() * _cacheBlockSize;
  }

  private long getBlockId(long pos) {
    return pos / _cacheBlockSize;
  }

  private long getBlockId() {
    return _position / _cacheBlockSize;
  }

  private void ensureOpen() {
    if (_isClosed) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

}
