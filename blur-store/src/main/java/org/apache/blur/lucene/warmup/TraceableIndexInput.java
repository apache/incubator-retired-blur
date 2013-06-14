package org.apache.blur.lucene.warmup;

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

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

public class TraceableIndexInput extends IndexInput {

  private final TraceableDirectory _traceableDirectory;
  private final String _name;
  private IndexInput _input;
  private boolean _traceOn;

  public TraceableIndexInput(TraceableDirectory traceableDirectory, String name, IOContext context, IndexInput input) {
    super(name);
    _traceableDirectory = traceableDirectory;
    _input = input;
    _traceOn = _traceableDirectory.isTrace();
    _name = name;
  }

  @Override
  public void close() throws IOException {
    _input.close();
  }

  @Override
  public long getFilePointer() {
    return _input.getFilePointer();
  }

  @Override
  public void seek(long pos) throws IOException {
    _input.seek(pos);
  }

  @Override
  public long length() {
    return _input.length();
  }

  @Override
  public byte readByte() throws IOException {
    trace();
    return _input.readByte();
  }

  private void trace() {
    if (_traceOn) {
      if (IndexWarmup.isRunTrace()) {
        _traceableDirectory.trace(_name, _input.getFilePointer());
      }
    }
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    trace();
    _input.readBytes(b, offset, len);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
    trace();
    _input.readBytes(b, offset, len, useBuffer);
  }

  @Override
  public IndexInput clone() {
    TraceableIndexInput clone = (TraceableIndexInput) super.clone();
    clone._input = _input.clone();
    clone._traceOn = _traceableDirectory.isTrace();
    return clone;
  }

}
