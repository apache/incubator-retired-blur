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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class BlurObjectInspectorGenerator {

  private static final String STORED = "stored";
  private static final String STRING = "string";
  private static final String TEXT = "text";
  private static final String LONG = "long";
  private static final String FLOAT = "float";
  private static final String INT = "int";
  private static final String DOUBLE = "double";
  private static final String DATE = "date";
  private static final String GEO_TERMPREFIX = "geo-termprefix";
  private static final String GEO_POINTVECTOR = "geo-pointvector";
  private static final String GEO_RECURSIVEPREFIX = "geo-recursiveprefix";
  private static final String LATITUDE = "latitude";
  private static final String LONGITUDE = "longitude";

  private static final Comparator<ColumnDefinition> COMPARATOR = new Comparator<ColumnDefinition>() {
    @Override
    public int compare(ColumnDefinition o1, ColumnDefinition o2) {
      return o1.getColumnName().compareTo(o2.getColumnName());
    }
  };

  private ObjectInspector _objectInspector;
  private List<String> _columnNames;
  private List<TypeInfo> _columnTypes;

  public BlurObjectInspectorGenerator(Collection<ColumnDefinition> colDefs) throws SerDeException {
    List<ColumnDefinition> colDefList = new ArrayList<ColumnDefinition>(colDefs);
    Collections.sort(colDefList, COMPARATOR);
    for (ColumnDefinition columnDefinition : colDefList) {
      _columnNames.add(columnDefinition.getColumnName());
      _columnTypes.add(getTypeInfo(columnDefinition));
    }
    _objectInspector = createObjectInspector();
  }

  private ObjectInspector createObjectInspector() throws SerDeException {
    List<ObjectInspector> columnObjectInspectorList = new ArrayList<ObjectInspector>(_columnNames.size());
    for (int i = 0; i < _columnNames.size(); i++) {
      columnObjectInspectorList.add(i, createObjectInspectorWorker(_columnTypes.get(i)));
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(_columnNames, columnObjectInspectorList);
  }

  private ObjectInspector createObjectInspectorWorker(TypeInfo ti) throws SerDeException {
    switch (ti.getCategory()) {
    case PRIMITIVE:
      PrimitiveTypeInfo pti = (PrimitiveTypeInfo) ti;
      return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);
    case STRUCT:
      StructTypeInfo sti = (StructTypeInfo) ti;
      ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>(sti.getAllStructFieldTypeInfos().size());
      for (TypeInfo typeInfo : sti.getAllStructFieldTypeInfos()) {
        ois.add(createObjectInspectorWorker(typeInfo));
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(sti.getAllStructFieldNames(), ois);
    default:
      throw new SerDeException("No Hive categories matched for [" + ti + "]");
    }
  }

  private TypeInfo getTypeInfo(ColumnDefinition columnDefinition) throws SerDeException {
    String fieldType = columnDefinition.getFieldType();
    if (fieldType.equals(TEXT) || fieldType.equals(STRING) || fieldType.equals(STORED)) {
      return TypeInfoFactory.stringTypeInfo;
    } else if (fieldType.equals(LONG)) {
      return TypeInfoFactory.longTypeInfo;
    } else if (fieldType.equals(INT)) {
      return TypeInfoFactory.intTypeInfo;
    } else if (fieldType.equals(FLOAT)) {
      return TypeInfoFactory.floatTypeInfo;
    } else if (fieldType.equals(DOUBLE)) {
      return TypeInfoFactory.doubleTypeInfo;
    } else if (fieldType.equals(DATE)) {
      return TypeInfoFactory.dateTypeInfo;
    } else if (fieldType.equals(GEO_POINTVECTOR)) {
      return TypeInfoFactory.dateTypeInfo;
    } else if (fieldType.equals(GEO_POINTVECTOR) || fieldType.equals(GEO_RECURSIVEPREFIX)
        || fieldType.equals(GEO_TERMPREFIX)) {
      List<TypeInfo> typeInfos = Arrays.asList((TypeInfo) TypeInfoFactory.floatTypeInfo,
          (TypeInfo) TypeInfoFactory.floatTypeInfo);
      TypeInfoFactory.getStructTypeInfo(Arrays.asList(LONGITUDE, LATITUDE), typeInfos);
    }
    throw new SerDeException("Blur Field Type [" + fieldType + "] is not supported.");
  }

  public ObjectInspector getObjectInspector() {
    return _objectInspector;
  }

  public List<String> getColumnNames() {
    return _columnNames;
  }

  public List<TypeInfo> getColumnTypes() {
    return _columnTypes;
  }

}
