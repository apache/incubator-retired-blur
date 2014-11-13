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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.IndexInput;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

public class HdfsStreamIndexInput extends IndexInput {

  private final long _length;
  private final FSDataInputStream _inputStream;
  private final MetricsGroup _metricsGroup;
  private final Histogram _readStreamAccess;
  private final Meter _readStreamThroughput;
  private final Meter _readStreamSeek;

  private long _postion;

  public HdfsStreamIndexInput(FSDataInputStream inputStream, long length, MetricsGroup metricsGroup, Path path)
      throws IOException {
    super("HdfsStreamIndexInput(" + path.toString() + ")");
    _inputStream = inputStream;
    _length = length;
    _metricsGroup = metricsGroup;
    _postion = _inputStream.getPos();
    _readStreamSeek = _metricsGroup.readStreamSeek;
    _readStreamAccess = _metricsGroup.readStreamAccess;
    _readStreamThroughput = _metricsGroup.readStreamThroughput;
  }

  private void checkPosition() throws IOException {
    long pos = _inputStream.getPos();
    if (pos != _postion) {
      _inputStream.seek(_postion);
      _readStreamSeek.mark();
    }
  }

  @Override
  public IndexInput clone() {
    if (IndexInputMergeUtil.isMergeThread()) {
      return super.clone();
    }
    throw new RuntimeException("who is doing this?");
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public long getFilePointer() {
    return _postion;
  }

  @Override
  public void seek(long pos) throws IOException {
    _postion = pos;
  }

  @Override
  public long length() {
    return _length;
  }

  @Override
  public byte readByte() throws IOException {
    long start = System.nanoTime();
    synchronized (_inputStream) {
      checkPosition();
      try {
        return _inputStream.readByte();
      } finally {
        _postion++;
        addMetric(start, 1);
      }
    }
  }

  private void addMetric(long start, int length) {
    long end = System.nanoTime();
    _readStreamAccess.update((end - start) / 1000);
    _readStreamThroughput.mark(length);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    long start = System.nanoTime();
    synchronized (_inputStream) {
      checkPosition();
      try {
        _inputStream.read(b, offset, len);
      } finally {
        _postion += len;
        addMetric(start, len);
      }
    }
  }

  @Override
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
    readBytes(b, offset, len);
  }

  @Override
  public short readShort() throws IOException {
    long start = System.nanoTime();
    synchronized (_inputStream) {
      checkPosition();
      try {
        return _inputStream.readShort();
      } finally {
        _postion += 2;
        addMetric(start, 2);
      }
    }
  }

  @Override
  public int readInt() throws IOException {
    long start = System.nanoTime();
    synchronized (_inputStream) {
      checkPosition();
      try {
        return _inputStream.readInt();
      } finally {
        _postion += 4;
        addMetric(start, 4);
      }
    }
  }

  @Override
  public long readLong() throws IOException {
    long start = System.nanoTime();
    synchronized (_inputStream) {
      checkPosition();
      try {
        return _inputStream.readLong();
      } finally {
        _postion += 8;
        addMetric(start, 8);
      }
    }
  }

}
