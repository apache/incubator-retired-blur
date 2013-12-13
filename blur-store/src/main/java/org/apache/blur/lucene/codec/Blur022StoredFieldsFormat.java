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

import static org.apache.blur.utils.BlurConstants.FAST;
import static org.apache.blur.utils.BlurConstants.FAST_DECOMPRESSION;
import static org.apache.blur.utils.BlurConstants.HIGH_COMPRESSION;

import java.io.IOException;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsReader;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weigher;

public final class Blur022StoredFieldsFormat extends StoredFieldsFormat {

  static final String STORED_FIELDS_FORMAT_CHUNK_SIZE = "StoredFieldsFormat.chunkSize";
  static final String STORED_FIELDS_FORMAT_COMPRESSION_MODE = "StoredFieldsFormat.compressionMode";
  private static final String FORMAT_NAME = "Blur022StoredFields";
  private static final String SEGMENT_SUFFIX = "";
  private final int _chunkSize;
  private final CompressionMode _compressionMode;
  private final ConcurrentLinkedHashMap<CachedKey, BytesRef> _cache;
  private final int capacity = 16 * 1024 * 1024;

  public Blur022StoredFieldsFormat(int chunkSize, CompressionMode compressionMode) {
    _chunkSize = chunkSize;
    _compressionMode = compressionMode;
    _cache = new ConcurrentLinkedHashMap.Builder<CachedKey, BytesRef>().weigher(new Weigher<BytesRef>() {
      @Override
      public int weightOf(BytesRef value) {
        return value.bytes.length;
      }
    }).maximumWeightedCapacity(capacity).build();
  }

  static class CachedCompressionMode extends CompressionMode {

    final CompressionMode _compressionMode;
    final SegmentInfo _si;
    final ConcurrentLinkedHashMap<CachedKey, BytesRef> _cache;

    CachedCompressionMode(CompressionMode compressionMode, SegmentInfo si,
        ConcurrentLinkedHashMap<CachedKey, BytesRef> cache) {
      _compressionMode = compressionMode;
      _si = si;
      _cache = cache;
    }

    @Override
    public Compressor newCompressor() {
      return _compressionMode.newCompressor();
    }

    @Override
    public Decompressor newDecompressor() {
      return new CachedDecompressor(_compressionMode.newDecompressor(), _si, _cache);
    }

    @Override
    public String toString() {
      return _compressionMode.toString();
    }

  }

  @Override
  public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context)
      throws IOException {
    CompressionMode compressionMode = new CachedCompressionMode(getCompressionMode(si), si, _cache);
    return new CompressingStoredFieldsReader(directory, si, SEGMENT_SUFFIX, fn, context, FORMAT_NAME, compressionMode);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
    return new CompressingStoredFieldsWriter(directory, si, SEGMENT_SUFFIX, context, FORMAT_NAME, _compressionMode,
        _chunkSize);
  }

  private CompressionMode getCompressionMode(SegmentInfo si) {
    String attribute = si.getAttribute(STORED_FIELDS_FORMAT_COMPRESSION_MODE);
    if (HIGH_COMPRESSION.equals(attribute)) {
      return CompressionMode.HIGH_COMPRESSION;
    } else if (FAST.equals(attribute)) {
      return CompressionMode.FAST;
    } else if (FAST_DECOMPRESSION.equals(attribute)) {
      return CompressionMode.FAST_DECOMPRESSION;
    }
    // This happen during nrt udpates.
    return _compressionMode;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
