package org.apache.blur.store.hdfs;

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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.DataOutput;

public class HdfsFileWriter extends DataOutput {

  public static final int VERSION = -1;

  private FSDataOutputStream _outputStream;
  private HdfsMetaBlock _block;
  private List<HdfsMetaBlock> _blocks = new ArrayList<HdfsMetaBlock>();
  private long _length;
  private long _currentPosition;

  public HdfsFileWriter(FileSystem fileSystem, Path path) throws IOException {
    _outputStream = fileSystem.create(path);
    seek(0);
  }

  public long length() {
    return _length;
  }

  public void seek(long pos) throws IOException {
    if (_block != null) {
      _blocks.add(_block);
    }
    _block = new HdfsMetaBlock();
    _block.realPosition = _outputStream.getPos();
    _block.logicalPosition = pos;
    _currentPosition = pos;
  }

  public void close() throws IOException {
    if (_block != null) {
      _blocks.add(_block);
    }
    flushMetaBlocks();
    _outputStream.close();
  }

  private void flushMetaBlocks() throws IOException {
    for (HdfsMetaBlock block : _blocks) {
      block.write(_outputStream);
    }
    _outputStream.writeInt(_blocks.size());
    _outputStream.writeLong(length());
    _outputStream.writeInt(VERSION);
  }

  @Override
  public void writeByte(byte b) throws IOException {
    _outputStream.write(b & 0xFF);
    _block.length++;
    _currentPosition++;
    updateLength();
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    _outputStream.write(b, offset, length);
    _block.length += length;
    _currentPosition += length;
    updateLength();
  }

  private void updateLength() {
    if (_currentPosition > _length) {
      _length = _currentPosition;
    }
  }

  public long getPosition() {
    return _currentPosition;
  }
}
