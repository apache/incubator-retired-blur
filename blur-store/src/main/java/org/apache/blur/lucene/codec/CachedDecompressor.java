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
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

public class CachedDecompressor extends Decompressor {

  private final Decompressor _decompressor;
  private final ConcurrentLinkedHashMap<CachedKey, BytesRef> _cache;
  private final SegmentInfo _si;

  public CachedDecompressor(Decompressor decompressor, SegmentInfo si,
      ConcurrentLinkedHashMap<CachedKey, BytesRef> cache) {
    _si = si;
    _decompressor = decompressor;
    _cache = cache;
  }

  @Override
  public void decompress(final DataInput in, final int originalLength, final int offset, final int length,
      final BytesRef bytes) throws IOException {
    if (in instanceof IndexInput) {
      IndexInput indexInput = (IndexInput) in;
      String name = indexInput.toString();
      long filePointer = indexInput.getFilePointer();
      CachedKey key = new CachedKey(name, filePointer, _si);
      BytesRef cachedRef = _cache.get(key);
      if (cachedRef == null) {
        cachedRef = new BytesRef(originalLength + 7);
        _decompressor.decompress(indexInput, originalLength, 0, originalLength, cachedRef);
        _cache.put(key, cachedRef);
        cachedRef.length = originalLength;
        cachedRef.offset = 0;
      }
      if (bytes.bytes.length < originalLength + 7) {
        bytes.bytes = new byte[ArrayUtil.oversize(originalLength + 7, 1)];
      }
      System.arraycopy(cachedRef.bytes, cachedRef.offset, bytes.bytes, 0, length + offset);
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
