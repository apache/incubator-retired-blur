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
package org.apache.blur.store.hdfs;

import java.io.IOException;

import org.apache.blur.store.buffer.ReusedBufferedIndexInput;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.IndexInput;

public class HdfsIndexInput extends ReusedBufferedIndexInput {

  private final long _length;
  private final FSDataInputStream _inputStream;
  private final MetricsGroup _metricsGroup;
  private final Path _path;
  private final HdfsDirectory _dir;
  private final boolean _sequentialReadAllowed;

  private long _prevFilePointer;
  private long _sequentialReadDetectorCounter;
  private long _sequentialReadThreshold = 50;
  private boolean _sequentialRead;
  private FSDataInputStream _sequentialInputStream;

  public HdfsIndexInput(HdfsDirectory dir, FSDataInputStream inputStream, long length, MetricsGroup metricsGroup,
      Path path, boolean sequentialReadAllowed) throws IOException {
    super("HdfsIndexInput(" + path.toString() + ")");
    _dir = dir;
    _inputStream = inputStream;
    _length = length;
    _metricsGroup = metricsGroup;
    _path = path;
    _sequentialReadAllowed = sequentialReadAllowed;
  }

  @Override
  public long length() {
    return _length;
  }

  @Override
  protected void seekInternal(long pos) throws IOException {

  }

  @Override
  protected void readInternal(byte[] b, int offset, int length) throws IOException {
    Tracer trace = Trace.trace("filesystem - read", Trace.param("file", _path),
        Trace.param("location", getFilePointer()), Trace.param("length", length));
    try {
      long start = System.nanoTime();
      long filePointer = getFilePointer();
      if (!_sequentialReadAllowed) {
        randomAccessRead(b, offset, length, start, filePointer);
        return;
      }
      if (filePointer == _prevFilePointer) {
        _sequentialReadDetectorCounter++;
      } else {
        if (_sequentialRead) {
          // System.out.println("Sequential Read OFF clone [" + _isClone + "] ["
          // + _path + "] count ["
          // + (_sequentialReadDetectorCounter - _sequentialReadThreshold) +
          // "]");
        }
        _sequentialReadDetectorCounter = 0;
        _sequentialRead = false;
      }
      if (_sequentialReadDetectorCounter > _sequentialReadThreshold && !_sequentialRead) {
        // System.out.println("Sequential Read ON clone [" + _isClone + "] [" +
        // _path + "]");
        _sequentialRead = true;
        if (_sequentialInputStream == null) {
          _sequentialInputStream = _dir.openForSequentialInput(_path, this);
        }
      }
      if (_sequentialRead) {
        long pos = _sequentialInputStream.getPos();
        if (pos != filePointer) {
          _sequentialInputStream.seek(filePointer);
        }
        _sequentialInputStream.readFully(b, offset, length);
        // @TODO add metrics back
      } else {
        filePointer = randomAccessRead(b, offset, length, start, filePointer);
      }
      _prevFilePointer = filePointer;
    } finally {
      trace.done();
    }
  }

  private long randomAccessRead(byte[] b, int offset, int length, long start, long filePointer) throws IOException {
    int olen = length;
    while (length > 0) {
      int amount;
      amount = _inputStream.read(filePointer, b, offset, length);
      length -= amount;
      offset += amount;
      filePointer += amount;
    }
    long end = System.nanoTime();
    _metricsGroup.readRandomAccess.update((end - start) / 1000);
    _metricsGroup.readRandomThroughput.mark(olen);
    return filePointer;
  }

  @Override
  public IndexInput clone() {
    HdfsIndexInput clone = (HdfsIndexInput) super.clone();
    clone._sequentialInputStream = null;
    return clone;
  }

  @Override
  protected void closeInternal() throws IOException {

  }
}
