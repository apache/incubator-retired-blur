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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.utils.ThreadValue;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

public class BlurSerializer {

  private static final String DATE_FORMAT = "dateFormat";
  private static final String DATE = "date";
  private Map<String, ThreadValue<SimpleDateFormat>> _dateFormat = new HashMap<String, ThreadValue<SimpleDateFormat>>();
  private BlurColumnNameResolver _columnNameResolver;

  public BlurSerializer(Map<String, ColumnDefinition> colDefs, BlurColumnNameResolver columnNameResolver) {
    _columnNameResolver = columnNameResolver;
    Set<Entry<String, ColumnDefinition>> entrySet = colDefs.entrySet();
    for (Entry<String, ColumnDefinition> e : entrySet) {
      String columnName = e.getKey();
      ColumnDefinition columnDefinition = e.getValue();
      String fieldType = columnDefinition.getFieldType();
      if (fieldType.equals(DATE)) {
        Map<String, String> properties = columnDefinition.getProperties();
        final String dateFormat = properties.get(DATE_FORMAT);
        ThreadValue<SimpleDateFormat> threadLocal = new ThreadValue<SimpleDateFormat>() {
          @Override
          protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat(dateFormat);
          }
        };
        _dateFormat.put(columnName, threadLocal);
      }
    }
  }

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

    if (structFieldsDataAsList.size() != size) {
      throw new SerDeException("Number of input columns was different than output columns (in = "
          + structFieldsDataAsList.size() + " vs out = " + size);
    }

    for (int i = 0; i < size; i++) {
      String columnName = _columnNameResolver.fromHiveToBlur(columnNames.get(i));
      StructField structFieldRef = outputFieldRefs.get(i);
      ObjectInspector fieldOI = structFieldRef.getFieldObjectInspector();
      Object structFieldData = structFieldsDataAsList.get(i);
      add(blurRecord, columnName, fieldOI, structFieldData);
    }
    return blurRecord;
  }

  private void add(BlurRecord blurRecord, String columnName, ObjectInspector objectInspector, Object data)
      throws SerDeException {
    if (data == null) {
      return;
    }
    if (objectInspector instanceof PrimitiveObjectInspector) {
      PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;
      String strValue = toString(columnName, data, primitiveObjectInspector);
      if (columnName.equals(BlurObjectInspectorGenerator.ROWID)) {
        blurRecord.setRowId(strValue);
      } else if (columnName.equals(BlurObjectInspectorGenerator.RECORDID)) {
        blurRecord.setRecordId(strValue);
      } else {
        blurRecord.addColumn(columnName, strValue);
      }
    } else if (objectInspector instanceof StructObjectInspector) {
      StructObjectInspector structObjectInspector = (StructObjectInspector) objectInspector;
      Map<String, StructField> allStructFieldRefs = toMap(structObjectInspector.getAllStructFieldRefs());
      String latitude = getFieldData(columnName, data, structObjectInspector, allStructFieldRefs,
          BlurObjectInspectorGenerator.LATITUDE);
      String longitude = getFieldData(columnName, data, structObjectInspector, allStructFieldRefs,
          BlurObjectInspectorGenerator.LONGITUDE);
      blurRecord.addColumn(columnName, toLatLong(latitude, longitude));
    } else if (objectInspector instanceof ListObjectInspector) {
      ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
      List<?> list = listObjectInspector.getList(data);
      ObjectInspector listElementObjectInspector = listObjectInspector.getListElementObjectInspector();
      for (Object obj : list) {
        add(blurRecord, columnName, listElementObjectInspector, obj);
      }
    } else {
      throw new SerDeException("ObjectInspector [" + objectInspector + "] of type ["
          + (objectInspector != null ? objectInspector.getClass() : null) + "] not supported.");
    }
  }

  private String getFieldData(String columnName, Object data, StructObjectInspector structObjectInspector,
      Map<String, StructField> allStructFieldRefs, String name) throws SerDeException {
    StructField structField = allStructFieldRefs.get(name);
    ObjectInspector fieldObjectInspector = structField.getFieldObjectInspector();
    Object structFieldData = structObjectInspector.getStructFieldData(data, structField);
    if (fieldObjectInspector instanceof PrimitiveObjectInspector) {
      return toString(columnName, structFieldData, (PrimitiveObjectInspector) fieldObjectInspector);
    } else {
      throw new SerDeException("Embedded non-primitive type is not supported columnName [" + columnName
          + "] objectInspector [" + fieldObjectInspector + "].");
    }
  }

  private String toString(String columnName, Object data, PrimitiveObjectInspector primitiveObjectInspector)
      throws SerDeException {
    Object primitiveJavaObject = primitiveObjectInspector.getPrimitiveJavaObject(data);
    return toString(columnName, primitiveJavaObject);
  }

  private String toLatLong(String latitude, String longitude) throws SerDeException {
    return toString(BlurObjectInspectorGenerator.LATITUDE, latitude) + ","
        + toString(BlurObjectInspectorGenerator.LONGITUDE, longitude);
  }

  private Map<String, StructField> toMap(List<? extends StructField> allStructFieldRefs) {
    Map<String, StructField> map = new HashMap<String, StructField>();
    for (StructField structField : allStructFieldRefs) {
      map.put(structField.getFieldName(), structField);
    }
    return map;
  }

  private String toString(String columnName, Object o) throws SerDeException {
    if (o == null) {
      return null;
    } else if (o instanceof String) {
      return o.toString();
    } else if (o instanceof Long) {
      return ((Long) o).toString();
    } else if (o instanceof Integer) {
      return ((Integer) o).toString();
    } else if (o instanceof Float) {
      return ((Float) o).toString();
    } else if (o instanceof Double) {
      return ((Double) o).toString();
    } else if (o instanceof Date) {
      SimpleDateFormat simpleDateFormat = getSimpleDateFormat(columnName);
      return simpleDateFormat.format((Date) o);
    } else {
      throw new SerDeException("Unknown type [" + o + "] with class [" + o == null ? "unknown" : o.getClass() + "]");
    }
  }

  private SimpleDateFormat getSimpleDateFormat(String columnName) throws SerDeException {
    ThreadValue<SimpleDateFormat> threadLocal = _dateFormat.get(columnName);
    if (threadLocal == null) {
      throw new SerDeException("Date format missing for column [" + columnName + "]");
    }
    return threadLocal.get();
  }
}
