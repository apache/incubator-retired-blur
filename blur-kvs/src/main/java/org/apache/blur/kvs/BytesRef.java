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
package org.apache.blur.kvs;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.WritableComparator;

public class BytesRef implements Comparable<BytesRef> {

  private static final String UTF_8 = "UTF-8";

  public static final byte[] EMPTY_BYTES = new byte[0];

  public byte[] bytes;
  public int offset;
  public int length;

  public BytesRef(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  public BytesRef() {
    this(EMPTY_BYTES);
  }

  public BytesRef(byte[] b) {
    this(b, 0, b.length);
  }

  public BytesRef(String s) {
    this(toBytes(s));
  }

  public BytesRef(int capacity) {
    this.bytes = new byte[capacity];
  }

  private static byte[] toBytes(String s) {
    try {
      return s.getBytes(UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static BytesRef deepCopyOf(BytesRef other) {
    BytesRef copy = new BytesRef();
    copy.copyBytes(other);
    return copy;
  }

  public void copyBytes(BytesRef other) {
    if (bytes.length - offset < other.length) {
      bytes = new byte[other.length];
      offset = 0;
    }
    System.arraycopy(other.bytes, other.offset, bytes, offset, other.length);
    length = other.length;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    final int end = offset + length;
    for (int i = offset; i < end; i++) {
      hash = 31 * hash + bytes[i];
    }
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other instanceof BytesRef) {
      return this.bytesEquals((BytesRef) other);
    }
    return false;
  }

  public boolean bytesEquals(BytesRef other) {
    assert other != null;
    if (length == other.length) {
      int otherUpto = other.offset;
      final byte[] otherBytes = other.bytes;
      final int end = offset + length;
      for (int upto = offset; upto < end; upto++, otherUpto++) {
        if (bytes[upto] != otherBytes[otherUpto]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(BytesRef o) {
    return WritableComparator.compareBytes(bytes, offset, length, o.bytes, o.offset, o.length);
  }

  public String utf8ToString() {
    try {
      return new String(bytes, offset, length, UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

}
