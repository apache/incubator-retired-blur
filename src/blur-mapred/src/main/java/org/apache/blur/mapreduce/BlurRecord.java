package org.apache.blur.mapreduce;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.utils.ReaderBlurRecord;
import org.apache.hadoop.io.Writable;


public class BlurRecord implements Writable, ReaderBlurRecord {

  private String _rowId;
  private String _recordId;
  private String _family;

  private List<BlurColumn> _columns = new ArrayList<BlurColumn>();

  @Override
  public void readFields(DataInput in) throws IOException {
    _rowId = IOUtil.readString(in);
    _recordId = IOUtil.readString(in);
    _family = IOUtil.readString(in);
    int size = IOUtil.readVInt(in);
    _columns.clear();
    for (int i = 0; i < size; i++) {
      BlurColumn column = new BlurColumn();
      column.readFields(in);
      _columns.add(column);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    IOUtil.writeString(out, _rowId);
    IOUtil.writeString(out, _recordId);
    IOUtil.writeString(out, _family);
    IOUtil.writeVInt(out, _columns.size());
    for (BlurColumn column : _columns) {
      column.write(out);
    }
  }

  public String getRowId() {
    return _rowId;
  }

  public void setRowId(String rowId) {
    this._rowId = rowId;
  }

  public String getRecordId() {
    return _recordId;
  }

  public void setRecordId(String recordId) {
    this._recordId = recordId;
  }

  public String getFamily() {
    return _family;
  }

  public void setFamily(String family) {
    this._family = family;
  }

  public List<BlurColumn> getColumns() {
    return _columns;
  }

  public void setColumns(List<BlurColumn> columns) {
    this._columns = columns;
  }

  public void clearColumns() {
    _columns.clear();
  }

  public void addColumn(BlurColumn column) {
    _columns.add(column);
  }

  public void addColumn(String name, String value) {
    BlurColumn blurColumn = new BlurColumn();
    blurColumn.setName(name);
    blurColumn.setValue(value);
    addColumn(blurColumn);
  }

  @Override
  public void setRecordIdStr(String value) {
    setRecordId(value);
  }

  @Override
  public void setFamilyStr(String family) {
    setFamily(family);
  }

  public void reset() {
    clearColumns();
    _rowId = null;
    _recordId = null;
    _family = null;
  }

  @Override
  public void setRowIdStr(String rowId) {
    setRowId(rowId);
  }

  @Override
  public String toString() {
    return "{rowId=" + _rowId + ", recordId=" + _recordId + ", family=" + _family + ", columns=" + _columns + "}";
  }

}
