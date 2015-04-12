package org.apache.blur.mapreduce.lib;

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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.hadoop.io.Writable;

/**
 * {@link BlurMutate} carries the {@link Record}s bound for the {@link Row} for
 * indexing. If this mutate represents a delete of the {@link Row} the recordId
 * of the {@link BlurRecord} is ignored.
 */
public class BlurMutate implements Writable {

  /**
   * The {@link MUTATE_TYPE} controls the mutating of the {@link Row}. DELETE
   * indicates that the {@link Row} is to be deleted. REPLACE indicates that the
   * group of mutates are to replace the existing {@link Row}.
   * 
   * If both a DELETE and a REPLACE exist for a single {@link Row} in the
   * {@link BlurOutputFormat} then the {@link Row} will be replaced not just
   * deleted.
   */
  public enum MUTATE_TYPE {
    /* ADD(0), UPDATE(1), */DELETE(2), REPLACE(3);
    private int _value;

    private MUTATE_TYPE(int value) {
      _value = value;
    }

    public int getValue() {
      return _value;
    }

    public MUTATE_TYPE find(int value) {
      switch (value) {
      // @TODO Updates through MR is going to be disabled
      // case 0:
      // return ADD;
      // case 1:
      // return UPDATE;
      case 2:
        return DELETE;
      case 3:
        return REPLACE;
      default:
        throw new RuntimeException("Value [" + value + "] not found.");
      }
    }
  }

  private MUTATE_TYPE _mutateType = MUTATE_TYPE.REPLACE;
  private BlurRecord _record = new BlurRecord();

  public BlurMutate() {

  }

  public BlurMutate(MUTATE_TYPE type, BlurRecord record) {
    _mutateType = type;
    _record = record;
  }

  public BlurMutate(MUTATE_TYPE type, String rowId) {
    _mutateType = type;
    _record.setRowId(rowId);
  }

  public BlurMutate(MUTATE_TYPE type, String rowId, String recordId) {
    _mutateType = type;
    _record.setRowId(rowId);
    _record.setRecordId(recordId);
  }

  public BlurMutate(MUTATE_TYPE type, String rowId, String recordId, String family) {
    _mutateType = type;
    _record.setRowId(rowId);
    _record.setRecordId(recordId);
    _record.setFamily(family);
  }

  public BlurMutate addColumn(BlurColumn column) {
    _record.addColumn(column);
    return this;
  }

  public BlurMutate addColumn(String name, String value) {
    return addColumn(new BlurColumn(name, value));
  }

  public BlurRecord getRecord() {
    return _record;
  }

  public void setRecord(BlurRecord record) {
    _record = record;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    IOUtil.writeVInt(out, _mutateType.getValue());
    _record.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    _mutateType.find(IOUtil.readVInt(in));
    _record.readFields(in);
  }

  public MUTATE_TYPE getMutateType() {
    return _mutateType;
  }

  public BlurMutate setMutateType(MUTATE_TYPE mutateType) {
    _mutateType = mutateType;
    return this;
  }

  @Override
  public String toString() {
    return "BlurMutate [mutateType=" + _mutateType + ", record=" + _record + "]";
  }

  public BlurMutate setFamily(String family) {
    _record.setFamily(family);
    return this;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_mutateType == null) ? 0 : _mutateType.hashCode());
    result = prime * result + ((_record == null) ? 0 : _record.hashCode());
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
    BlurMutate other = (BlurMutate) obj;
    if (_mutateType != other._mutateType)
      return false;
    if (_record == null) {
      if (other._record != null)
        return false;
    } else if (!_record.equals(other._record))
      return false;
    return true;
  }

}
