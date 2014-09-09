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
package org.apache.blur.mapreduce.lib.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.lucene.util.BytesRef;

public class LuceneKeyWritable implements WritableComparable<LuceneKeyWritable> {

  public static enum Type {
    SHARD_FIELD_TEXT((byte) 0), SHARD_FIELD_TEXT_DOCUMENTID((byte) 1), SHARD_FIELD_TEXT_DOCUMENTID_POSITION((byte) 2);

    private final byte _value;

    private Type(byte value) {
      _value = value;
    }

    public byte value() {
      return _value;
    }

    public static Type lookup(byte value) {
      switch (value) {
      case 0:
        return SHARD_FIELD_TEXT;
      case 1:
        return SHARD_FIELD_TEXT_DOCUMENTID;
      case 2:
        return SHARD_FIELD_TEXT_DOCUMENTID_POSITION;
      default:
        throw new RuntimeException("Value [" + value + "] not found.");
      }
    }
  }

  private int _shardId;
  private int _fieldId;
  private BytesRef _text = new BytesRef();
  private Type _type;
  private int _documentId;
  private int _position;

  public LuceneKeyWritable() {

  }

  public LuceneKeyWritable(int shardId, int fieldId, BytesRef text, Type type, int documentId, int position) {
    _shardId = shardId;
    _fieldId = fieldId;
    _text = text;
    _type = type;
    _documentId = documentId;
    _position = position;
  }

  public int getShardId() {
    return _shardId;
  }

  public int getFieldId() {
    return _fieldId;
  }

  public BytesRef getText() {
    return _text;
  }

  public Type getType() {
    return _type;
  }

  public int getDocumentId() {
    return _documentId;
  }

  public int getPosition() {
    return _position;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    _shardId = in.readInt();
    _fieldId = in.readInt();
    read(in, _text);
    _type = Type.lookup(in.readByte());
    switch (_type) {
    case SHARD_FIELD_TEXT:
      return;
    case SHARD_FIELD_TEXT_DOCUMENTID:
      _documentId = in.readInt();
      return;
    case SHARD_FIELD_TEXT_DOCUMENTID_POSITION:
      _documentId = in.readInt();
      _position = in.readInt();
      return;
    default:
      throw new IOException("Type [" + _type + "] not supported.");
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(_shardId);
    out.writeInt(_fieldId);
    write(out, _text);
    out.writeByte(_type.value());
    switch (_type) {
    case SHARD_FIELD_TEXT:
      return;
    case SHARD_FIELD_TEXT_DOCUMENTID:
      out.writeInt(_documentId);
      return;
    case SHARD_FIELD_TEXT_DOCUMENTID_POSITION:
      out.writeInt(_documentId);
      out.writeInt(_position);
      return;
    default:
      throw new IOException("Type [" + _type + "] not supported.");
    }
  }

  private void write(DataOutput out, BytesRef ref) throws IOException {
    out.writeInt(ref.length);
    out.write(ref.bytes, ref.offset, ref.length);
  }

  private void read(DataInput in, BytesRef ref) throws IOException {
    int len = in.readInt();
    if (ref.bytes.length < len) {
      ref.grow(len);
    }
    in.readFully(ref.bytes, 0, len);
    ref.offset = 0;
    ref.length = len;
  }

  @Override
  public int compareTo(LuceneKeyWritable o) {
    int compareTo = _shardId - o._shardId;
    if (compareTo == 0) {
      compareTo = _fieldId - o._fieldId;
      if (compareTo == 0) {
        compareTo = _text.compareTo(o._text);
        switch (_type) {
        case SHARD_FIELD_TEXT:
          return compareTo;
        case SHARD_FIELD_TEXT_DOCUMENTID:
          return _documentId - o._documentId;
        case SHARD_FIELD_TEXT_DOCUMENTID_POSITION: {
          compareTo = _documentId - o._documentId;
          if (compareTo == 0) {
            return _position - o._position;
          }
          return compareTo;
        }
        default:
          throw new RuntimeException("Type [" + _type + "] not supported.");
        }
      }
    }
    return compareTo;
  }
}
