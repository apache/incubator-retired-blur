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

public class HdfsRandomAccessIndexInput extends ReusedBufferedIndexInput {

  private final long _length;
  private final FSDataInputStream _inputStream;
  private final MetricsGroup _metricsGroup;
  private final Path _path;

  public HdfsRandomAccessIndexInput(FSDataInputStream inputStream, long length, MetricsGroup metricsGroup, Path path) throws IOException {
    super("HdfsRandomAccessIndexInput(" + path.toString() + ")");
    _inputStream = inputStream;
    _length = length;
    _metricsGroup = metricsGroup;
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
    } finally {
      trace.done();
    }
  }

  @Override
  public IndexInput clone() {
    return (HdfsRandomAccessIndexInput) super.clone();
  }

  @Override
  protected void closeInternal() throws IOException {

  }
}
