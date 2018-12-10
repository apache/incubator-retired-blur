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
package org.apache.blur.mapreduce.lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TableBlurRecord implements Writable {

  private Text _table = new Text();
  private BlurRecord _blurRecord = new BlurRecord();

  public TableBlurRecord() {

  }

  public TableBlurRecord(String table, BlurRecord blurRecord) {
    this(new Text(table), blurRecord);
  }

  public TableBlurRecord(Text table, BlurRecord blurRecord) {
    _table = table;
    _blurRecord = blurRecord;
  }

  public TableBlurRecord(TableBlurRecord tableBlurRecord) {
    _table = new Text(tableBlurRecord.getTable());
    _blurRecord = new BlurRecord(tableBlurRecord.getBlurRecord());
  }

  public Text getTable() {
    return _table;
  }

  public void setTable(Text table) {
    _table = table;
  }

  public BlurRecord getBlurRecord() {
    return _blurRecord;
  }

  public void setBlurRecord(BlurRecord blurRecord) {
    _blurRecord = blurRecord;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    _table.write(out);
    _blurRecord.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    _table.readFields(in);
    _blurRecord.readFields(in);
  }

  @Override
  public String toString() {
    return "TableBlurRecord [_table=" + _table + ", _blurRecord=" + _blurRecord + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_blurRecord == null) ? 0 : _blurRecord.hashCode());
    result = prime * result + ((_table == null) ? 0 : _table.hashCode());
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
    TableBlurRecord other = (TableBlurRecord) obj;
    if (_blurRecord == null) {
      if (other._blurRecord != null)
        return false;
    } else if (!_blurRecord.equals(other._blurRecord))
      return false;
    if (_table == null) {
      if (other._table != null)
        return false;
    } else if (!_table.equals(other._table))
      return false;
    return true;
  }

}
