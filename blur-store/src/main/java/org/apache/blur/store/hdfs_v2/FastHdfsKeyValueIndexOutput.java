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

import org.apache.lucene.store.IndexOutput;

public class FastHdfsKeyValueIndexOutput extends IndexOutput {

  private long _position;
  private byte[] _buffer;
  private int _bufferPosition;
  private final int _blockSize;
  private final FastHdfsKeyValueDirectory _dir;
  private final String _name;
  private boolean _closed;

  public FastHdfsKeyValueIndexOutput(String name, int blockSize, FastHdfsKeyValueDirectory dir) {
    _blockSize = blockSize;
    _buffer = new byte[blockSize];
    _name = name;
    _dir = dir;
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public void close() throws IOException {
    _closed = true;
    long blockId;
    if (_bufferPosition == _blockSize) {
      blockId = (_position - 1) / _blockSize;
    } else {
      blockId = (_position) / _blockSize;
    }
    _dir.writeBlock(_name, blockId, _buffer, 0, _bufferPosition);
    _dir.writeLength(_name, _position);
  }

  @Override
  public long getFilePointer() {
    return _position;
  }

  @Override
  public void seek(long pos) throws IOException {
    throw new IOException("not supported");
  }

  @Override
  public long length() throws IOException {
    return _position;
  }

  @Override
  public void writeByte(byte b) throws IOException {
    ensureOpen();
    tryToFlush();
    _buffer[_bufferPosition] = b;
    _bufferPosition++;
    _position++;
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    ensureOpen();
    while (length > 0) {
      tryToFlush();
      int len = Math.min(_blockSize - _bufferPosition, length);
      System.arraycopy(b, offset, _buffer, _bufferPosition, len);
      _bufferPosition += len;
      _position += len;
      offset += len;
      length -= len;
    }
  }

  private void ensureOpen() throws IOException {
    if (_closed) {
      throw new IOException("Already closed.");
    }
  }

  private void tryToFlush() throws IOException {
    if (_bufferPosition > _blockSize) {
      throw new IOException("Problem");
    } else if (_bufferPosition == _blockSize) {
      long blockId = (_position - 1) / _blockSize;
      _dir.writeBlock(_name, blockId, _buffer, 0, _bufferPosition);
      _bufferPosition = 0;
    }
  }

}
