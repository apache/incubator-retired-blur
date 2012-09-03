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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.DataInput;


public class HdfsFileReader extends DataInput {

  private static final Log LOG = LogFactory.getLog(HdfsFileReader.class);

  private static final int VERSION = -1;

  private final long _length;
  private final long _hdfsLength;
  private final List<HdfsMetaBlock> _metaBlocks;
  private FSDataInputStream _inputStream;
  private long _logicalPos;
  private long _boundary;
  private long _realPos;
  private boolean isClone;

  public HdfsFileReader(FileSystem fileSystem, Path path, int bufferSize) throws IOException {
    if (!fileSystem.exists(path)) {
      throw new FileNotFoundException(path.toString());
    }
    FileStatus fileStatus = fileSystem.getFileStatus(path);
    _hdfsLength = fileStatus.getLen();
    _inputStream = fileSystem.open(path, bufferSize);

    // read meta blocks
    _inputStream.seek(_hdfsLength - 16);
    int numberOfBlocks = _inputStream.readInt();
    _length = _inputStream.readLong();
    int version = _inputStream.readInt();
    if (version != VERSION) {
      throw new RuntimeException("Version of file [" + version + "] does not match reader [" + VERSION + "]");
    }
    _inputStream.seek(_hdfsLength - 16 - (numberOfBlocks * 24)); // 3 longs per
                                                                 // block
    _metaBlocks = new ArrayList<HdfsMetaBlock>(numberOfBlocks);
    for (int i = 0; i < numberOfBlocks; i++) {
      HdfsMetaBlock hdfsMetaBlock = new HdfsMetaBlock();
      hdfsMetaBlock.readFields(_inputStream);
      _metaBlocks.add(hdfsMetaBlock);
    }
    seek(0);
  }

  public HdfsFileReader(FileSystem fileSystem, Path path) throws IOException {
    this(fileSystem, path, HdfsDirectory.BUFFER_SIZE);
  }

  public long getPosition() {
    return _logicalPos;
  }

  public long length() {
    return _length;
  }

  public void seek(long pos) throws IOException {
    if (_logicalPos == pos) {
      return;
    }
    _logicalPos = pos;
    seekInternal();
  }

  public void close() throws IOException {
    if (!isClone) {
      _inputStream.close();
    }
  }

  /**
   * This method should never be used!
   */
  @Override
  public byte readByte() throws IOException {
    LOG.warn("Should not be used!");
    byte[] buf = new byte[1];
    readBytes(buf, 0, 1);
    return buf[0];
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    checkBoundary();
    // might need to read in multiple stages
    while (len > 0) {
      if (_logicalPos >= _boundary) {
        seekInternal();
      }
      int lengthToRead = (int) Math.min(_boundary - _logicalPos, len);
      _inputStream.read(_realPos, b, offset, lengthToRead);
      offset += lengthToRead;
      _logicalPos += lengthToRead;
      _realPos += lengthToRead;
      len -= lengthToRead;
    }
  }

  private void checkBoundary() throws IOException {
    if (_boundary == -1l) {
      throw new IOException("eof");
    }
  }

  private void seekInternal() throws IOException {
    HdfsMetaBlock block = null;
    for (HdfsMetaBlock b : _metaBlocks) {
      if (b.containsDataAt(_logicalPos)) {
        block = b;
      }
    }
    if (block == null) {
      _boundary = -1l;
      _realPos = -1l;
    } else {
      _realPos = block.getRealPosition(_logicalPos);
      _boundary = getBoundary(block);
    }
  }

  private long getBoundary(HdfsMetaBlock block) {
    _boundary = block.logicalPosition + block.length;
    for (HdfsMetaBlock b : _metaBlocks) {
      if (b.logicalPosition > block.logicalPosition && b.logicalPosition < _boundary && b.logicalPosition >= _logicalPos) {
        _boundary = b.logicalPosition;
      }
    }
    return _boundary;
  }

  public static long getLength(FileSystem fileSystem, Path path) throws IOException {
    FSDataInputStream inputStream = null;
    try {
      FileStatus fileStatus = fileSystem.getFileStatus(path);
      inputStream = fileSystem.open(path);
      long hdfsLength = fileStatus.getLen();
      inputStream.seek(hdfsLength - 12);
      long length = inputStream.readLong();
      int version = inputStream.readInt();
      if (version != VERSION) {
        throw new RuntimeException("Version of file [" + version + "] does not match reader [" + VERSION + "]");
      }
      return length;
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }

  @Override
  public Object clone() {
    HdfsFileReader reader = (HdfsFileReader) super.clone();
    reader.isClone = true;
    return reader;
  }

}
