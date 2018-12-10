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
package org.apache.blur.mapreduce.lib.update;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class IndexKey implements WritableComparable<IndexKey> {

  public enum TYPE {
    NEW_DATA_MARKER((byte) 0), OLD_DATA((byte) 1), NEW_DATA((byte) 2);

    private byte _type;

    private TYPE(byte type) {
      _type = type;
    }

    public byte getType() {
      return _type;
    }

    public static TYPE findType(byte type) {
      switch (type) {
      case 0:
        return NEW_DATA_MARKER;
      case 1:
        return OLD_DATA;
      case 2:
        return NEW_DATA;
      default:
        throw new RuntimeException("Type [" + type + "] not found.");
      }
    }
  }

  private Text _rowId = new Text();
  private TYPE _type;
  private Text _recordId = new Text();
  private long _timestamp;

  public static IndexKey newData(String rowId, String recordId, long timestamp) {
    return newData(new Text(rowId), new Text(recordId), timestamp);
  }

  public static IndexKey newData(Text rowId, Text recordId, long timestamp) {
    IndexKey updateKey = new IndexKey();
    updateKey._rowId = rowId;
    updateKey._recordId = recordId;
    updateKey._timestamp = timestamp;
    updateKey._type = TYPE.NEW_DATA;
    return updateKey;
  }

  public static IndexKey newDataMarker(String rowId) {
    return newDataMarker(new Text(rowId));
  }

  public static IndexKey newDataMarker(Text rowId) {
    IndexKey updateKey = new IndexKey();
    updateKey._rowId = rowId;
    updateKey._type = TYPE.NEW_DATA_MARKER;
    return updateKey;
  }

  public static IndexKey oldData(String rowId, String recordId) {
    return oldData(new Text(rowId), new Text(recordId));
  }

  public static IndexKey oldData(Text rowId, Text recordId) {
    IndexKey updateKey = new IndexKey();
    updateKey._rowId = rowId;
    updateKey._recordId = recordId;
    updateKey._timestamp = 0L;
    updateKey._type = TYPE.OLD_DATA;
    return updateKey;
  }

  public Text getRowId() {
    return _rowId;
  }

  public TYPE getType() {
    return _type;
  }

  public Text getRecordId() {
    return _recordId;
  }

  public long getTimestamp() {
    return _timestamp;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    _rowId.write(out);
    out.write(_type.getType());
    switch (_type) {
    case NEW_DATA_MARKER:
      break;
    default:
      _recordId.write(out);
      out.writeLong(_timestamp);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    _rowId.readFields(in);
    _type = TYPE.findType(in.readByte());
    switch (_type) {
    case NEW_DATA_MARKER:
      _recordId = new Text();
      _timestamp = 0L;
      break;
    default:
      _recordId.readFields(in);
      _timestamp = in.readLong();
    }
  }

  @Override
  public int compareTo(IndexKey o) {
    int compareTo = _rowId.compareTo(o._rowId);
    if (compareTo == 0) {
      if (_type == TYPE.NEW_DATA_MARKER) {
        compareTo = _type.compareTo(o._type);
      } else {
        compareTo = _recordId.compareTo(o._recordId);
        if (compareTo == 0) {
          compareTo = _type.compareTo(o._type);
          if (compareTo == 0) {
            compareTo = compare(_timestamp, o._timestamp);
          }
        }
      }
    }
    return compareTo;
  }

  private int compare(long a, long b) {
    return (a < b) ? -1 : ((a == b) ? 0 : 1);
  }

  @Override
  public String toString() {
    return "IndexKey [_rowId=" + _rowId + ", _type=" + _type + ", _recordId=" + _recordId + ", _timestamp="
        + _timestamp + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_recordId == null) ? 0 : _recordId.hashCode());
    result = prime * result + ((_rowId == null) ? 0 : _rowId.hashCode());
    result = prime * result + (int) (_timestamp ^ (_timestamp >>> 32));
    result = prime * result + ((_type == null) ? 0 : _type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    IndexKey other = (IndexKey) obj;
    if (_recordId == null) {
      if (other._recordId != null)
        return false;
    } else if (!_recordId.equals(other._recordId))
      return false;
    if (_rowId == null) {
      if (other._rowId != null)
        return false;
    } else if (!_rowId.equals(other._rowId))
      return false;
    if (_timestamp != other._timestamp)
      return false;
    if (_type != other._type)
      return false;
    return true;
  }

}
