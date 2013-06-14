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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class IndexTracerResult {

  public enum FILE_TYPE {
    TIM, DOC, POS, PAY
  }

  private boolean _timCaptured;
  private long _timPosition;
  private String _timFileName;

  private boolean _docCaptured;
  private long _docPosition;
  private String _docFileName;

  private boolean _posCaptured;
  private long _posPosition;
  private String _posFileName;

  private boolean _payCaptured;
  private long _payPosition;
  private String _payFileName;

  private String _segmentName;
  private String _field;

  IndexTracerResult() {

  }

  public static IndexTracerResult read(IndexInput input) throws IOException {

    IndexTracerResult result = new IndexTracerResult();

    result._field = input.readString();
    result._segmentName = input.readString();

    result._timCaptured = readBoolean(input);
    if (result._timCaptured) {
      result._timPosition = input.readVLong();
      result._timFileName = input.readString();
    }

    result._docCaptured = readBoolean(input);
    if (result._docCaptured) {
      result._docPosition = input.readVLong();
      result._docFileName = input.readString();
    }

    result._posCaptured = readBoolean(input);
    if (result._posCaptured) {
      result._posPosition = input.readVLong();
      result._posFileName = input.readString();
    }

    result._payCaptured = readBoolean(input);
    if (result._payCaptured) {
      result._payPosition = input.readVLong();
      result._payFileName = input.readString();
    }
    return result;
  }

  public void write(IndexOutput output) throws IOException {
    output.writeString(_field);
    output.writeString(_segmentName);

    writeBoolean(output, _timCaptured);
    if (_timCaptured) {
      output.writeVLong(_timPosition);
      output.writeString(_timFileName);
    }

    writeBoolean(output, _docCaptured);
    if (_docCaptured) {
      output.writeVLong(_docPosition);
      output.writeString(_docFileName);
    }

    writeBoolean(output, _posCaptured);
    if (_posCaptured) {
      output.writeVLong(_posPosition);
      output.writeString(_posFileName);
    }

    writeBoolean(output, _payCaptured);
    if (_payCaptured) {
      output.writeVLong(_payPosition);
      output.writeString(_payFileName);
    }
  }

  private static boolean readBoolean(IndexInput input) throws IOException {
    return input.readVInt() == 1;
  }

  private static void writeBoolean(IndexOutput output, boolean b) throws IOException {
    output.writeVInt(b ? 1 : 0);
  }

  public IndexTracerResult(String segmentName, String field) {
    _segmentName = segmentName;
    _field = field;
  }

  public void setPosition(long position, String fileName, FILE_TYPE type) {
    switch (type) {
    case TIM:
      _timPosition = position;
      _timCaptured = true;
      _timFileName = fileName;
      break;
    case DOC:
      _docPosition = position;
      _docCaptured = true;
      _docFileName = fileName;
      break;
    case PAY:
      _payPosition = position;
      _payCaptured = true;
      _payFileName = fileName;
      break;
    case POS:
      _posPosition = position;
      _posCaptured = true;
      _posFileName = fileName;
      break;
    default:
      throw new NotSupported(type);
    }

  }

  public long getPosition(FILE_TYPE type) {
    switch (type) {
    case TIM:
      return _timPosition;
    case DOC:
      return _docPosition;
    case PAY:
      return _payPosition;
    case POS:
      return _posPosition;
    default:
      throw new NotSupported(type);
    }
  }

  public boolean isFilePositionCaptured(FILE_TYPE type) {
    switch (type) {
    case TIM:
      return _timCaptured;
    case DOC:
      return _docCaptured;
    case PAY:
      return _payCaptured;
    case POS:
      return _posCaptured;
    default:
      throw new NotSupported(type);
    }
  }

  public String getFileName(FILE_TYPE type) {
    switch (type) {
    case TIM:
      return _timFileName;
    case DOC:
      return _docFileName;
    case PAY:
      return _payFileName;
    case POS:
      return _posFileName;
    default:
      throw new NotSupported(type);
    }
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getField() {
    return _field;
  }

  @Override
  public String toString() {
    return "IndexTracerResult [_docCaptured=" + _docCaptured + ", _docPosition=" + _docPosition + ", _docFileName="
        + _docFileName + ", _posCaptured=" + _posCaptured + ", _posPosition=" + _posPosition + ", _posFileName="
        + _posFileName + ", _payCaptured=" + _payCaptured + ", _payPosition=" + _payPosition + ", _payFileName="
        + _payFileName + ", _timCaptured=" + _timCaptured + ", _timPosition=" + _timPosition + ", _timFileName="
        + _timFileName + ", _segmentName=" + _segmentName + ", _field=" + _field + "]";
  }

}
