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

public class HdfsIndexInput extends ReusedBufferedIndexInput {

  private final long _length;
  private FSDataInputStream _inputStream;
  private boolean _isClone;
  private final MetricsGroup _metricsGroup;
  private int _readVersion;
  private final Path _path;

  public HdfsIndexInput(String name, FSDataInputStream inputStream, long length, MetricsGroup metricsGroup,
      int readVersion, Path path) throws IOException {
    super("HdfsIndexInput(" + path.toString() + ")");
    _inputStream = inputStream;
    _length = length;
    _metricsGroup = metricsGroup;
    _readVersion = readVersion;
    _path = path;
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
      switch (_readVersion) {
      case 0:
        synchronized (_inputStream) {
          _inputStream.seek(getFilePointer());
          _inputStream.readFully(b, offset, length);
        }
        break;
      case 1:
        while (length > 0) {
          int amount;
          synchronized (_inputStream) {
            _inputStream.seek(filePointer);
            amount = _inputStream.read(b, offset, length);
          }
          length -= amount;
          offset += amount;
          filePointer += amount;
        }
        break;
      case 2:
        _inputStream.readFully(filePointer, b, offset, length);
        break;
      case 3:
        while (length > 0) {
          int amount;
          amount = _inputStream.read(filePointer, b, offset, length);
          length -= amount;
          offset += amount;
          filePointer += amount;
        }
        break;
      default:
        break;
      }
      long end = System.nanoTime();
      _metricsGroup.readAccess.update((end - start) / 1000);
      _metricsGroup.readThroughput.mark(length);
    } finally {
      trace.done();
    }
  }

  @Override
  protected void closeInternal() throws IOException {
    if (!_isClone) {
      _inputStream.close();
    }
  }

  @Override
  public ReusedBufferedIndexInput clone() {
    HdfsIndexInput clone = (HdfsIndexInput) super.clone();
    clone._isClone = true;
    clone._readVersion = HdfsDirectory.fetchImpl.get();
    return clone;
  }
}
