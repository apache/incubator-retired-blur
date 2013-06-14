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

import org.apache.blur.lucene.warmup.IndexTracerResult.FILE_TYPE;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;

public class IndexTracer {

  private boolean _hasOffsets;
  private boolean _hasPayloads;
  private boolean _hasPositions;
  private SegmentReader _segmentReader;
  private Bits _liveDocs;
  private IndexTracerResult _result;
  private TraceableDirectory _traceableDirectory;
  private final int _maxSampleSize;

  public IndexTracer(TraceableDirectory dir, int maxSampleSize) {
    _traceableDirectory = dir;
    _maxSampleSize = maxSampleSize;
  }

  public void initTrace(SegmentReader segmentReader, String field, boolean hasPositions, boolean hasPayloads,
      boolean hasOffsets) {
    _segmentReader = segmentReader;
    _hasPositions = hasPositions;
    _hasPayloads = hasPayloads;
    _hasOffsets = hasOffsets;
    _liveDocs = _segmentReader.getLiveDocs();
    _result = new IndexTracerResult(segmentReader.getSegmentName(), field);
  }

  public void trace(String name, long filePointer) {
    int index = name.lastIndexOf('.');
    String ext = name.substring(index);
    if (ext.endsWith(".tim")) {
      if (!_result.isFilePositionCaptured(FILE_TYPE.TIM)) {
        _result.setPosition(filePointer, name, FILE_TYPE.TIM);
      }
    } else if (ext.endsWith(".doc")) {
      if (!_result.isFilePositionCaptured(FILE_TYPE.DOC)) {
        _result.setPosition(filePointer, name, FILE_TYPE.DOC);
      }
    } else if (ext.endsWith(".pos")) {
      if (!_result.isFilePositionCaptured(FILE_TYPE.POS)) {
        _result.setPosition(filePointer, name, FILE_TYPE.POS);
      }
    } else {
      throw new RuntimeException("Not Implemented");
    }
  }

  public IndexTracerResult runTrace(Terms terms) throws IOException {
    IndexWarmup.enableRunTrace();
    _traceableDirectory.setIndexTracer(this);
    _traceableDirectory.setTrace(true);
    try {
      TermsEnum termsEnum = terms.iterator(null);
      int sampleCount = 0;
      while (termsEnum.next() != null) {
        if (_hasPositions || _hasOffsets || _hasPayloads) {
          DocsAndPositionsEnum docsAndPositions = termsEnum.docsAndPositions(_liveDocs, null);
          int nextDoc;
          do {
            nextDoc = docsAndPositions.nextDoc();
            int freq = docsAndPositions.freq();
            for (int i = 0; i < freq; i++) {
              docsAndPositions.nextPosition();
              if (_hasPayloads) {
                docsAndPositions.getPayload();
              }
              if (traceComplete()) {
                return getResult();
              }
            }
          } while (nextDoc != DocsEnum.NO_MORE_DOCS);
        } else {
          DocsEnum docsEnum = termsEnum.docs(_liveDocs, null);
          int nextDoc;
          do {
            nextDoc = docsEnum.nextDoc();
            if (traceComplete()) {
              return getResult();
            }
          } while (nextDoc != DocsEnum.NO_MORE_DOCS);
        }
        sampleCount++;
        if (sampleCount >= _maxSampleSize) {
          break;
        }
      }
      return getResult();
    } finally {
      _traceableDirectory.setTrace(false);
      IndexWarmup.disableRunTrace();
    }
  }

  private boolean traceComplete() {
    if (!_result.isFilePositionCaptured(FILE_TYPE.TIM)) {
      return false;
    }
    if (!_result.isFilePositionCaptured(FILE_TYPE.DOC)) {
      return false;
    }
    if (_hasPositions && !_result.isFilePositionCaptured(FILE_TYPE.POS)) {
      return false;
    }
    if (_hasPayloads && !_result.isFilePositionCaptured(FILE_TYPE.PAY)) {
      return false;
    }
    return true;
  }

  IndexTracerResult getResult() {
    return _result;
  }

}
