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
package org.apache.blur.lucene.codec;

import java.io.IOException;

import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

public class CachedDecompressor extends Decompressor {

  static class Entry {
    String _inputName;
    long _position = -1;
    BytesRef _bytesRef = new BytesRef();
  }

  private final Decompressor _decompressor;
  private final ThreadLocal<Entry> _cache = new ThreadLocal<Entry>() {
    @Override
    protected Entry initialValue() {
      return new Entry();
    }
  };

  public CachedDecompressor(Decompressor decompressor) {
    _decompressor = decompressor;
  }

  @Override
  public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
    if (in instanceof IndexInput) {
      IndexInput indexInput = (IndexInput) in;
      String name = indexInput.toString();
      Entry entry = _cache.get();
      long filePointer = indexInput.getFilePointer();
      BytesRef cachedRef = entry._bytesRef;
      if (!name.equals(entry._inputName) || entry._position != filePointer) {
        cachedRef.grow(originalLength);
        _decompressor.decompress(in, originalLength, 0, originalLength, cachedRef);
        entry._inputName = name;
        entry._position = filePointer;
      }
      bytes.copyBytes(cachedRef);
      bytes.offset = offset;
      bytes.length = length;
    } else {
      _decompressor.decompress(in, originalLength, offset, length, bytes);
    }
  }

  @Override
  public Decompressor clone() {
    return this;
  }
}
