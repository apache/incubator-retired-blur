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
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class BlurObjectInspectorGenerator {

  public static final String RECORDID = "recordid";
  public static final String ROWID = "rowid";
  public static final String STORED = "stored";
  public static final String STRING = "string";
  public static final String TEXT = "text";
  public static final String LONG = "long";
  public static final String FLOAT = "float";
  public static final String INT = "int";
  public static final String DOUBLE = "double";
  public static final String DATE = "date";
  public static final String GEO_TERMPREFIX = "geo-termprefix";
  public static final String GEO_POINTVECTOR = "geo-pointvector";
  public static final String GEO_RECURSIVEPREFIX = "geo-recursiveprefix";
  public static final String LATITUDE = "latitude";
  public static final String LONGITUDE = "longitude";

  private static final Comparator<ColumnDefinition> COMPARATOR = new Comparator<ColumnDefinition>() {
    @Override
    public int compare(ColumnDefinition o1, ColumnDefinition o2) {
      return o1.getColumnName().compareTo(o2.getColumnName());
    }
  };

  private ObjectInspector _objectInspector;
  private List<String> _columnNames = new ArrayList<String>();
  private List<TypeInfo> _columnTypes = new ArrayList<TypeInfo>();
  private BlurColumnNameResolver _columnNameResolver;

  public BlurObjectInspectorGenerator(Collection<ColumnDefinition> colDefs, BlurColumnNameResolver columnNameResolver)
      throws SerDeException {
    _columnNameResolver = columnNameResolver;
    List<ColumnDefinition> colDefList = new ArrayList<ColumnDefinition>(colDefs);
    Collections.sort(colDefList, COMPARATOR);

    _columnNames.add(ROWID);
    _columnTypes.add(TypeInfoFactory.stringTypeInfo);

    _columnNames.add(RECORDID);
    _columnTypes.add(TypeInfoFactory.stringTypeInfo);

    for (ColumnDefinition columnDefinition : colDefList) {
      String hiveColumnName = _columnNameResolver.fromBlurToHive(columnDefinition.getColumnName());
      _columnNames.add(hiveColumnName);
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
      List<ObjectInspector> ois = new ArrayList<ObjectInspector>(sti.getAllStructFieldTypeInfos().size());
      for (TypeInfo typeInfo : sti.getAllStructFieldTypeInfos()) {
        ois.add(createObjectInspectorWorker(typeInfo));
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(sti.getAllStructFieldNames(), ois);
    case LIST:
      ListTypeInfo lti = (ListTypeInfo) ti;
      TypeInfo listElementTypeInfo = lti.getListElementTypeInfo();
      return ObjectInspectorFactory.getStandardListObjectInspector(createObjectInspectorWorker(listElementTypeInfo));
    default:
      throw new SerDeException("No Hive categories matched for [" + ti + "]");
    }
  }

  private TypeInfo getTypeInfo(ColumnDefinition columnDefinition) throws SerDeException {
    String fieldType = columnDefinition.getFieldType();
    TypeInfo typeInfo = getTypeInfo(fieldType);
    if (columnDefinition.isMultiValueField()) {
      return TypeInfoFactory.getListTypeInfo(typeInfo);
    }
    return typeInfo;
  }

  private TypeInfo getTypeInfo(String fieldType) {
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
    } else if (fieldType.equals(GEO_POINTVECTOR) || fieldType.equals(GEO_RECURSIVEPREFIX)
        || fieldType.equals(GEO_TERMPREFIX)) {
      List<TypeInfo> typeInfos = Arrays.asList((TypeInfo) TypeInfoFactory.floatTypeInfo,
          (TypeInfo) TypeInfoFactory.floatTypeInfo);
      return TypeInfoFactory.getStructTypeInfo(Arrays.asList(LATITUDE, LONGITUDE), typeInfos);
    }
    // Return string for anything that is not a built in type.
    return TypeInfoFactory.stringTypeInfo;
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
