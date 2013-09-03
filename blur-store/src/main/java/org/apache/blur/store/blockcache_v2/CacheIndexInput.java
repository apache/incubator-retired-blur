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

import java.io.IOException;

import org.apache.blur.store.buffer.BufferStore;
import org.apache.lucene.store.IndexInput;

public class CacheIndexInput extends IndexInput {

  private final long _fileLength;
  private final long _fileId;
  private final int _cacheBlockSize;
  private final int _bufferSize;
  private final CacheDirectory _directory;
  private final String _fileName;
  private final IndexInput _indexInput;
  private final Cache _cache;
  private final CacheKey _key = new CacheKey();

  private CacheValue _cacheValue;
  private long _position;
  private int _blockPosition;

  public CacheIndexInput(CacheDirectory directory, String fileName, IndexInput indexInput, Cache cache) throws IOException {
    super(fileName);
    _directory = directory;
    _fileName = fileName;
    _indexInput = indexInput;
    _fileLength = indexInput.length();
    _cache = cache;

    _fileId = _cache.getFileId(_directory, _fileName);
    _cacheBlockSize = _cache.getCacheBlockSize(_directory, _fileName);
    _bufferSize = _cache.getFileBufferSize(_directory, _fileName);
    _key.setFileId(_fileId);
  }

  @Override
  public byte readByte() throws IOException {
    tryToFill();
    byte b = _cacheValue.read(_blockPosition);
    _position++;
    _blockPosition++;
    return b;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    while (len > 0) {
      tryToFill();
      int remaining = remaining();
      int length = Math.min(len, remaining);
      _cacheValue.read(_blockPosition, b, offset, length);
      offset += length;
      len -= length;
      _position += length;
      _blockPosition += length;
    }
  }

  private int remaining() {
    return _cacheValue.length() - _blockPosition;
  }

  private void tryToFill() throws IOException {
    if (_cacheValue == null) {
      fill();
    } else if (remaining() == 0) {
      releaseCache();
      fill();
    } else {
      return;
    }
  }

  private void releaseCache() {
    if (_cacheValue != null) {
      _cacheValue.decRef();
      _cacheValue = null;
    }
  }

  private void fill() throws IOException {
    _key.setBlockId(getBlockId());
    _cacheValue = _cache.get(_key);
    if (_cacheValue == null) {
      _cacheValue = _cache.newInstance(_directory, _fileName);
      long filePosition = getFilePosition();
      _indexInput.seek(filePosition);
      byte[] buffer = BufferStore.takeBuffer(_bufferSize);
      int len = (int) Math.min(_cacheBlockSize, _fileLength - filePosition);
      int cachePosition = 0;
      while (len > 0) {
        int length = Math.min(_bufferSize, len);
        _indexInput.readBytes(buffer, 0, length);
        _cacheValue.write(cachePosition, buffer, 0, length);
        len -= length;
        cachePosition += length;
      }
    }
    _cache.put(_key.clone(), _cacheValue);
    _cacheValue.incRef();
    _blockPosition = getBlockPosition();
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

  @Override
  public void close() throws IOException {
    _indexInput.close();
  }

  @Override
  public long getFilePointer() {
    return _position;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (_position >= _fileLength) {
      throw new IOException("Can not seek past end of file [" + pos + "] filelength [" + _fileLength + "]");
    }
    long oldBlockId = getBlockId();
    _position = pos;
    long newBlockId = getBlockId(pos);
    if (newBlockId == oldBlockId) {
      // need to set new block position
      _blockPosition = getBlockPosition();
    } else {
      releaseCache();
    }
  }

  @Override
  public long length() {
    return _fileLength;
  }

}
