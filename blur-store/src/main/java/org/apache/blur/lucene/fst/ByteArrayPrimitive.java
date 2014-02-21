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
package org.apache.blur.lucene.fst;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

public class ByteArrayPrimitive extends ByteArray {

  private final byte[] _bytes;

  public ByteArrayPrimitive(int size) {
    _bytes = new byte[size];
  }

  @Override
  public int length() {
    return _bytes.length;
  }

  @Override
  public void put(int position, byte b) {
    _bytes[position] = b;
  }

  @Override
  public byte get(int position) {
    return _bytes[position];
  }

  @Override
  public void put(int position, byte[] b, int offset, int len) {
    System.arraycopy(b, offset, _bytes, position, len);
  }

  @Override
  public void get(int position, byte[] b, int offset, int len) {
    System.arraycopy(_bytes, position, b, offset, len);
  }

  @Override
  public void readBytes(DataInput in, int offset, int length) throws IOException {
    in.readBytes(_bytes, offset, length);
  }

  @Override
  public void writeBytes(DataOutput out, int offset, int length) throws IOException {
    out.writeBytes(_bytes, offset, length);
  }

  @Override
  public void copy(int position, ByteArray dest, int destOffset, int len) {
    System.arraycopy(_bytes, position, ((ByteArrayPrimitive) dest)._bytes, destOffset, len);
  }

}
