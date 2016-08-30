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

import org.apache.blur.utils.ThreadValue;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

public class CachedDecompressor extends Decompressor {

  private final Decompressor _decompressor;
  private final ThreadValue<Entry> _entry = new ThreadValue<Entry>() {
    @Override
    protected Entry initialValue() {
      return new Entry();
    }
  };

  public CachedDecompressor(Decompressor decompressor) {
    _decompressor = decompressor;
  }

  @Override
  public void decompress(final DataInput in, final int originalLength, final int offset, final int length,
      final BytesRef bytes) throws IOException {
    if (in instanceof IndexInput) {
      IndexInput indexInput = (IndexInput) in;
      String name = indexInput.toString();
      long filePointer = indexInput.getFilePointer();

      Entry entry = _entry.get();
      if (!entry.isValid(indexInput, name, filePointer)) {
        entry.setup(indexInput, name, filePointer);
        entry._cache.grow(originalLength + 7);
        _decompressor.decompress(indexInput, originalLength, 0, originalLength, entry._cache);
        entry._cache.length = originalLength;
        entry._cache.offset = 0;
        _entry.set(entry);
      }
      if (bytes.bytes.length < originalLength + 7) {
        bytes.bytes = new byte[ArrayUtil.oversize(originalLength + 7, 1)];
      }
      System.arraycopy(entry._cache.bytes, entry._cache.offset, bytes.bytes, 0, length + offset);
      bytes.offset = offset;
      bytes.length = length;
    } else {
      _decompressor.decompress(in, originalLength, offset, length, bytes);
    }
  }

  @Override
  public Decompressor clone() {
    return new CachedDecompressor(_decompressor.clone());
  }

  static class Entry {
    String _name;
    long _filePointer = -1;
    BytesRef _cache = new BytesRef();
    int _indexInputHashCode;

    void setup(IndexInput indexInput, String name, long filePointer) {
      _indexInputHashCode = System.identityHashCode(indexInput);
      _name = name;
      _filePointer = filePointer;
    }

    boolean isValid(IndexInput indexInput, String name, long filePointer) {
      if (_indexInputHashCode != System.identityHashCode(indexInput)) {
        return false;
      }
      if (!_name.equals(name)) {
        return false;
      }
      if (_filePointer != filePointer) {
        return false;
      }
      return true;
    }
  }
}
