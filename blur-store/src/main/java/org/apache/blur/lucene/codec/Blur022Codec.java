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

import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_CHUNKSIZE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_COMPRESSIONMODE;
import static org.apache.blur.utils.BlurConstants.FAST;
import static org.apache.blur.utils.BlurConstants.FAST_DECOMPRESSION;
import static org.apache.blur.utils.BlurConstants.HIGH_COMPRESSION;

import org.apache.blur.BlurConfiguration;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene42.Lucene42FieldInfosFormat;
import org.apache.lucene.codecs.lucene42.Lucene42NormsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42TermVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

@Deprecated
public class Blur022Codec extends Codec {

  private final StoredFieldsFormat fieldsFormat;
  private final TermVectorsFormat vectorsFormat = new Lucene42TermVectorsFormat();
  private final FieldInfosFormat fieldInfosFormat = new Lucene42FieldInfosFormat();
  private final SegmentInfoFormat infosFormat;
  private final LiveDocsFormat liveDocsFormat = new Blur021LiveDocsFormat();

  private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      return Blur022Codec.this.getPostingsFormatForField(field);
    }
  };

  private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
      return Blur022Codec.this.getDocValuesFormatForField(field);
    }
  };

  public Blur022Codec() {
    this(1 << 14, CompressionMode.FAST);
  }

  public Blur022Codec(int chunkSize, CompressionMode compressionMode) {
    super("Blur022");
    infosFormat = new Blur022SegmentInfoFormat(chunkSize, compressionMode);
    fieldsFormat = new Blur022StoredFieldsFormat(chunkSize, compressionMode);
  }

  public Blur022Codec(BlurConfiguration configuration) {
    this(configuration.getInt(BLUR_SHARD_INDEX_CHUNKSIZE, 1 << 14), getCompressionMode(configuration));
  }

  private static CompressionMode getCompressionMode(BlurConfiguration configuration) {
    String type = configuration.get(BLUR_SHARD_INDEX_COMPRESSIONMODE, FAST);
    if (HIGH_COMPRESSION.equals(type)) {
      return CompressionMode.HIGH_COMPRESSION;
    } else if (FAST.equals(type)) {
      return CompressionMode.FAST;
    } else if (FAST_DECOMPRESSION.equals(type)) {
      return CompressionMode.FAST_DECOMPRESSION;
    } else {
      throw new IllegalArgumentException("blur.shard.index.compressionmode=" + type
          + " not supported.  Valid entries are [FAST,FAST_DECOMPRESSION,HIGH_COMPRESSION]");
    }
  }

  @Override
  public final StoredFieldsFormat storedFieldsFormat() {
    return fieldsFormat;
  }

  @Override
  public final TermVectorsFormat termVectorsFormat() {
    return vectorsFormat;
  }

  @Override
  public final PostingsFormat postingsFormat() {
    return postingsFormat;
  }

  @Override
  public final FieldInfosFormat fieldInfosFormat() {
    return fieldInfosFormat;
  }

  @Override
  public final SegmentInfoFormat segmentInfoFormat() {
    return infosFormat;
  }

  @Override
  public final LiveDocsFormat liveDocsFormat() {
    return liveDocsFormat;
  }

  /**
   * Returns the postings format that should be used for writing new segments of
   * <code>field</code>.
   * 
   * The default implementation always returns "Lucene41"
   */
  public PostingsFormat getPostingsFormatForField(String field) {
    return defaultFormat;
  }

  /**
   * Returns the docvalues format that should be used for writing new segments
   * of <code>field</code>.
   * 
   * The default implementation always returns "Lucene42"
   */
  public DocValuesFormat getDocValuesFormatForField(String field) {
    return defaultDVFormat;
  }

  @Override
  public final DocValuesFormat docValuesFormat() {
    return docValuesFormat;
  }

  private final PostingsFormat defaultFormat = PostingsFormat.forName("Lucene41");
  private final DocValuesFormat defaultDVFormat = DocValuesFormat.forName("Disk");

  private final NormsFormat normsFormat = new Lucene42NormsFormat();

  @Override
  public final NormsFormat normsFormat() {
    return normsFormat;
  }
}
