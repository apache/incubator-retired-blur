package com.nearinfinity.blur.store.hdfs;

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

import org.apache.lucene.store.IndexOutput;

public class NullIndexOutput extends IndexOutput {

  private long _pos;
  private long _length;

  @Override
  public void close() throws IOException {

  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public long getFilePointer() {
    return _pos;
  }

  @Override
  public long length() throws IOException {
    return _length;
  }

  @Override
  public void seek(long pos) throws IOException {
    _pos = pos;
  }

  @Override
  public void writeByte(byte b) throws IOException {
    _pos++;
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    _pos += length;
    updateLength();
  }

  private void updateLength() {
    if (_pos > _length) {
      _length = _pos;
    }
  }

}
