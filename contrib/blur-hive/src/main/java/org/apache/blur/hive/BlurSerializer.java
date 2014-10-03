/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.blur.hive;

import java.util.List;
import java.util.Map;

import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

public class BlurSerializer {

  public Writable serialize(Object o, ObjectInspector objectInspector, List<String> columnNames,
      List<TypeInfo> columnTypes, Map<String, ColumnDefinition> schema, String family) throws SerDeException {
    BlurRecord blurRecord = new BlurRecord();
    blurRecord.setFamily(family);

    StructObjectInspector soi = (StructObjectInspector) objectInspector;

    List<? extends StructField> outputFieldRefs = soi.getAllStructFieldRefs();
    int size = columnNames.size();
    if (outputFieldRefs.size() != size) {
      throw new SerDeException("Number of input columns was different than output columns (in = " + size + " vs out = "
          + outputFieldRefs.size());
    }
    List<Object> structFieldsDataAsList = soi.getStructFieldsDataAsList(o);
    for (int i = 0; i < size; i++) {
      // StructField structFieldRef = outputFieldRefs.get(i);
      Object structFieldData = structFieldsDataAsList.get(i);
      if (structFieldData == null) {
        continue;
      }
      // ObjectInspector fieldOI = structFieldRef.getFieldObjectInspector();
      String columnName = columnNames.get(i);
      if (columnName.equals(BlurObjectInspectorGenerator.ROWID)) {
        blurRecord.setRowId((String) structFieldData);
      } else if (columnName.equals(BlurObjectInspectorGenerator.RECORDID)) {
        blurRecord.setRecordId((String) structFieldData);
      } else {
        if (columnName.equals(BlurObjectInspectorGenerator.GEO_POINTVECTOR)
            || columnName.equals(BlurObjectInspectorGenerator.GEO_RECURSIVEPREFIX)
            || columnName.equals(BlurObjectInspectorGenerator.GEO_TERMPREFIX)) {
          throw new SerDeException("Not supported yet.");
        } else {
          blurRecord.addColumn(columnName, toString(structFieldData));
        }
      }
    }
    return blurRecord;
  }

  private String toString(Object o) {
    return o.toString();
  }

}
