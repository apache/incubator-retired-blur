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

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.buffer.ReusedBufferedIndexInput;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.lucene.store.IndexInput;

public class HdfsIndexInput extends ReusedBufferedIndexInput {

  private static final Log LOG = LogFactory.getLog(HdfsIndexInput.class);

  private final long _length;
  private final FSDataInputRandomAccess _input;
  private final MetricsGroup _metricsGroup;
  private final String _name;
  private final HdfsDirectory _dir;

  private SequentialReadControl _sequentialReadControl;

  private long _prevFilePointer;
  private FSDataInputSequentialAccess _sequentialInput;

  public HdfsIndexInput(HdfsDirectory dir, FSDataInputRandomAccess input, long length, MetricsGroup metricsGroup,
      String name, SequentialReadControl sequentialReadControl) throws IOException {
    super("HdfsIndexInput(" + name + "@" + "" + input + ")");
    _sequentialReadControl = sequentialReadControl;
    _dir = dir;
    _input = input;
    _length = length;
    _metricsGroup = metricsGroup;
    _name = name;
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
    long start = System.nanoTime();
    long filePointer = getFilePointer();
    if (!_sequentialReadControl.isSequentialReadAllowed()) {
      randomAccessRead(b, offset, length, start, filePointer);
      return;
    }
    if (filePointer == _prevFilePointer) {
      _sequentialReadControl.incrReadDetector();
    } else {
      if (_sequentialReadControl.isEnabled()) {
        if (_sequentialReadControl.shouldSkipInput(filePointer, _prevFilePointer)) {
          _sequentialInput.skip(filePointer - _prevFilePointer);
        } else {
          LOG.debug("Current Pos [{0}] Prev Pos [{1}] Diff [{2}]", filePointer, _prevFilePointer, filePointer
              - _prevFilePointer);
          _sequentialReadControl.reset();
        }
      }
    }
    if (_sequentialReadControl.switchToSequentialRead()) {
      _sequentialReadControl.setEnabled(true);
      if (_sequentialInput == null) {
        Tracer trace = Trace.trace("filesystem - read - openForSequentialInput", Trace.param("file", toString()),
            Trace.param("location", getFilePointer()));
        _sequentialInput = _dir.openForSequentialInput(_name, this);
        trace.done();
      }
    }
    if (_sequentialReadControl.isEnabled()) {
      long pos = _sequentialInput.getPos();
      if (pos != filePointer) {
        _sequentialInput.seek(filePointer);
      }
      _sequentialInput.readFully(b, offset, length);
      filePointer = _sequentialInput.getPos();
      // @TODO add metrics back
    } else {
      filePointer = randomAccessRead(b, offset, length, start, filePointer);
    }
    _prevFilePointer = filePointer;
  }

  private long randomAccessRead(byte[] b, int offset, int length, long start, long filePointer) throws IOException {
    Tracer trace = Trace.trace("filesystem - read - randomAccessRead", Trace.param("file", toString()),
        Trace.param("location", getFilePointer()), Trace.param("length", length));
    try {
      int olen = length;
      while (length > 0) {
        int amount;
        amount = _input.read(filePointer, b, offset, length);
        length -= amount;
        offset += amount;
        filePointer += amount;
      }
      long end = System.nanoTime();
      _metricsGroup.readRandomAccess.update((end - start) / 1000);
      _metricsGroup.readRandomThroughput.mark(olen);
      return filePointer;
    } finally {
      trace.done();
    }
  }

  @Override
  public IndexInput clone() {
    HdfsIndexInput clone = (HdfsIndexInput) super.clone();
    clone._sequentialInput = null;
    clone._sequentialReadControl = _sequentialReadControl.clone();
    clone._sequentialReadControl.reset();
    return clone;
  }

  @Override
  protected void closeInternal() throws IOException {

  }
}
