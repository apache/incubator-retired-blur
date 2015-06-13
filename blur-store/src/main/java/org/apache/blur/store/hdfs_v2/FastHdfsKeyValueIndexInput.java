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
package org.apache.blur.store.hdfs_v2;

import java.io.IOException;

import org.apache.blur.kvs.BytesRef;
import org.apache.lucene.store.IndexInput;

public class FastHdfsKeyValueIndexInput extends IndexInput {

  private final int _blockSize;
  private final FastHdfsKeyValueDirectory _dir;
  private final String _name;

  private long _position;
  private long _length;
  private BytesRef _currentBlock;
  private long _currentBlockId = -1l;
  private boolean _closed;
  private int _bufferPosition = -1;

  public FastHdfsKeyValueIndexInput(String name, long length, int blockSize, FastHdfsKeyValueDirectory dir) {
    super(name);
    _name = name;
    _length = length;
    _blockSize = blockSize;
    _dir = dir;
    _currentBlock = new BytesRef(blockSize);
  }

  @Override
  public void close() throws IOException {
    _closed = true;
  }

  @Override
  public long getFilePointer() {
    return _position;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos != _position) {
      _position = pos;
      _bufferPosition = -1;
    }
  }

  @Override
  public long length() {
    return _length;
  }

  @Override
  public byte readByte() throws IOException {
    ensureOpen();
    checkEOF(1);
    setupRead();
    byte b = _currentBlock.bytes[_bufferPosition];
    _bufferPosition++;
    _position++;
    return b;
  }

  private void setupRead() throws IOException {
    long blockId = _position / _blockSize;
    if (_currentBlockId != blockId) {
      // read
      _dir.readBlock(_name, blockId, _currentBlock);
      _currentBlockId = blockId;
      _bufferPosition = (int) (_position % _blockSize);
    }
    // incase the seek was in the same block, check to see if _bufferPosition is
    // correct.
    if (_bufferPosition == -1) {
      _bufferPosition = (int) (_position % _blockSize);
    }
  }

  private void ensureOpen() throws IOException {
    if (_closed) {
      throw new IOException("Already closed.");
    }
  }

  @Override
  public void readBytes(byte[] b, int offset, int length) throws IOException {
    ensureOpen();
    checkEOF(length);
    while (length > 0) {
      setupRead();
      int len = Math.min(_blockSize - _bufferPosition, length);
      System.arraycopy(_currentBlock.bytes, _bufferPosition, b, offset, len);
      _position += len;
      offset += len;
      length -= len;
      _bufferPosition += len;
    }
  }

  private void checkEOF(int length) throws IOException {
    if (_position + length > _length) {
      throw new IOException("EOF reached.");
    }
  }

  @Override
  public IndexInput clone() {
    FastHdfsKeyValueIndexInput clone = (FastHdfsKeyValueIndexInput) super.clone();
    clone._currentBlock = new BytesRef();
    clone._currentBlockId = -1;
    clone._bufferPosition = -1;
    return clone;
  }

}
