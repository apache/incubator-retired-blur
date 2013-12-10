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
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.SegmentInfoWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

public class Blur022SegmentInfoWriter extends SegmentInfoWriter {

  private final String _compressionMode;
  private final int _compressionChunkSize;

  public Blur022SegmentInfoWriter(int compressionChunkSize, CompressionMode compressionMode) {
    _compressionChunkSize = compressionChunkSize;
    _compressionMode = compressionMode.toString();
  }

  @Override
  public void write(Directory dir, SegmentInfo si, FieldInfos fis, IOContext ioContext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(si.name, "", Blur022SegmentInfoFormat.SI_EXTENSION);
    si.addFile(fileName);

    final IndexOutput output = dir.createOutput(fileName, ioContext);

    boolean success = false;
    try {
      CodecUtil.writeHeader(output, Blur022SegmentInfoFormat.CODEC_NAME, Blur022SegmentInfoFormat.VERSION_CURRENT);
      output.writeString(si.getVersion());
      output.writeInt(si.getDocCount());

      output.writeByte((byte) (si.getUseCompoundFile() ? SegmentInfo.YES : SegmentInfo.NO));
      output.writeStringStringMap(si.getDiagnostics());
      Map<String, String> attributes = si.attributes();
      TreeMap<String, String> newAttributes = new TreeMap<String, String>();
      if (attributes != null) {
        newAttributes.putAll(attributes);
      }
      newAttributes.put(Blur022StoredFieldsFormat.STORED_FIELDS_FORMAT_CHUNK_SIZE,
          Integer.toString(_compressionChunkSize));
      newAttributes.put(Blur022StoredFieldsFormat.STORED_FIELDS_FORMAT_COMPRESSION_MODE, _compressionMode);
      output.writeStringStringMap(newAttributes);
      output.writeStringSet(si.files());

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(output);
        si.dir.deleteFile(fileName);
      } else {
        output.close();
      }
    }
  }
}
