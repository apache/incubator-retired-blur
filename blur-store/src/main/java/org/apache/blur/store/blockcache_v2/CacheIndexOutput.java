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
import org.apache.lucene.store.IndexOutput;

public class CacheIndexOutput extends IndexOutput {

  private final IndexOutput _indexOutput;
  private final int _fileBufferSize;
  private final Cache _cache;
  private final String _fileName;
  private final CacheDirectory _directory;
  private final long _fileId;

  private long _position;
  private byte[] _buffer;
  private int _bufferPosition;
  private int _cacheBlockSize;

  public CacheIndexOutput(CacheDirectory directory, String fileName, IndexOutput indexOutput, Cache cache)
      throws IOException {
    _cache = cache;
    _directory = directory;
    _fileName = fileName;
    _fileBufferSize = _cache.getFileBufferSize(_directory, _fileName);
    _cacheBlockSize = _cache.getCacheBlockSize(_directory, _fileName);
    _fileId = _cache.getFileId(_directory, _fileName);
    _indexOutput = indexOutput;
    _buffer = BufferStore.takeBuffer(_cacheBlockSize);
  }

  @Override
  public void writeByte(byte b) throws IOException {
    tryToFlush();
    _buffer[_bufferPosition] = b;
    _bufferPosition++;
    _position++;
  }

  @Override
  public void writeBytes(byte[] b, int offset, int len) throws IOException {
    while (len > 0) {
      tryToFlush();
      int remaining = remaining();
      int length = Math.min(len, remaining);
      System.arraycopy(b, offset, _buffer, _bufferPosition, length);
      _bufferPosition += length;
      _position += length;
      len -= length;
      offset += length;
    }
  }

  private int remaining() {
    return _cacheBlockSize - _bufferPosition;
  }

  private void tryToFlush() throws IOException {
    if (remaining() == 0) {
      flushInternal();
    }
  }

  private void flushInternal() throws IOException {
    CacheValue cacheValue = _cache.newInstance(_directory, _fileName);
    int length = _cacheBlockSize - (_cacheBlockSize - _bufferPosition);
    int l = length;
    int o = 0;
    while (l > 0) {
      int il = Math.min(_fileBufferSize, l);
      _indexOutput.writeBytes(_buffer, o, il);
      o += il;
      l -= il;
    }
    cacheValue.write(0, _buffer, 0, length);
    _cache.put(new CacheKey(_fileId, getBlockId()), cacheValue);
    _bufferPosition = 0;
  }

  private long getBlockId() {
    return _position / _cacheBlockSize;
  }

  @Override
  public void seek(long pos) throws IOException {
    throw new IOException("Seek is not supported.");
  }

  @Override
  public void close() throws IOException {
    flushInternal();
    _indexOutput.flush();
    _indexOutput.close();
    BufferStore.putBuffer(_buffer);
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public long getFilePointer() {
    return _position;
  }

  @Override
  public long length() throws IOException {
    return getFilePointer();
  }
}
