package org.apache.blur.analysis;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.analysis.type.DoubleFieldTypeDefinition;
import org.apache.blur.analysis.type.FloatFieldTypeDefinition;
import org.apache.blur.analysis.type.IntFieldTypeDefinition;
import org.apache.blur.analysis.type.LongFieldTypeDefinition;
import org.apache.blur.analysis.type.StoredFieldTypeDefinition;
import org.apache.blur.analysis.type.StringFieldTypeDefinition;
import org.apache.blur.analysis.type.TextFieldTypeDefinition;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;

public abstract class BaseFieldManager extends FieldManager {

  private static final Log LOG = LogFactory.getLog(BaseFieldManager.class);
  private static final Map<String, String> EMPTY_MAP = new HashMap<String, String>();;

  private Map<String, FieldTypeDefinition> _fieldNameToDefMap = new ConcurrentHashMap<String, FieldTypeDefinition>();
  private Map<String, Class<? extends FieldTypeDefinition>> _typeMap = new ConcurrentHashMap<String, Class<? extends FieldTypeDefinition>>();
  private Map<String, Set<String>> _columnToSubColumn = new ConcurrentHashMap<String, Set<String>>();

  public BaseFieldManager() {
    _typeMap.put(TextFieldTypeDefinition.NAME, TextFieldTypeDefinition.class);
    _typeMap.put(StringFieldTypeDefinition.NAME, StringFieldTypeDefinition.class);
    _typeMap.put(StoredFieldTypeDefinition.NAME, StoredFieldTypeDefinition.class);
    _typeMap.put(IntFieldTypeDefinition.NAME, IntFieldTypeDefinition.class);
    _typeMap.put(LongFieldTypeDefinition.NAME, LongFieldTypeDefinition.class);
    _typeMap.put(DoubleFieldTypeDefinition.NAME, DoubleFieldTypeDefinition.class);
    _typeMap.put(FloatFieldTypeDefinition.NAME, FloatFieldTypeDefinition.class);
  }

  @Override
  public Iterable<? extends Field> getFields(String rowId, Record record) {
    List<Field> fields = new ArrayList<Field>();
    String family = record.getFamily();
    List<Column> columns = record.getColumns();
    addDefaultFields(fields, rowId, record);
    for (Column column : columns) {
      getAndAddFields(fields, family, column, getFieldTypeDefinition(family, column));
      Collection<String> subColumns = getSubColumns(family, column);
      if (subColumns != null) {
        for (String subName : subColumns) {
          getAndAddFields(fields, family, column, subName, getFieldTypeDefinition(family, column, subName));
        }
      }
    }
    return fields;
  }

  private void getAndAddFields(List<Field> fields, String family, Column column, String subName,
      FieldTypeDefinition fieldTypeDefinition) {
    for (Field field : fieldTypeDefinition.getFieldsForSubColumn(family, column, subName)) {
      fields.add(field);
    }
  }

  private void addDefaultFields(List<Field> fields, String rowId, Record record) {
    String family = record.getFamily();
    String recordId = record.getRecordId();

    validateNotNull(rowId, "rowId");
    validateNotNull(family, "family");
    validateNotNull(recordId, "recordId");

    fields.add(new StringField(BlurConstants.FAMILY, family, Store.YES));
    fields.add(new StringField(BlurConstants.ROW_ID, rowId, Store.YES));
    fields.add(new StringField(BlurConstants.RECORD_ID, recordId, Store.YES));
  }

  private void validateNotNull(String value, String fieldName) {
    if (value != null) {
      return;
    }
    throw new IllegalArgumentException("Field [" + fieldName + "] cannot be null.");
  }

  private void getAndAddFields(List<Field> fields, String family, Column column, FieldTypeDefinition fieldTypeDefinition) {
    for (Field field : fieldTypeDefinition.getFieldsForColumn(family, column)) {
      fields.add(field);
    }
    if (fieldTypeDefinition.isFieldLessIndexing()) {
      addFieldLessIndex(fields, column.getValue());
    }
  }

  private void addFieldLessIndex(List<Field> fields, String value) {
    fields.add(new Field(BlurConstants.SUPER, value, TextFieldTypeDefinition.TYPE_NOT_STORED));
  }

  private FieldTypeDefinition getFieldTypeDefinition(String family, Column column, String subName) {
    return _fieldNameToDefMap.get(getSubColumnName(family, column, subName));
  }

  private String getSubColumnName(String family, Column column, String subName) {
    return getColumnName(family, column) + "." + subName;
  }

  private String getColumnName(String family, Column column) {
    return getColumnName(family, column.getName());
  }

  private String getColumnName(String family, String columnName) {
    return family + "." + columnName;
  }

  private Collection<String> getSubColumns(String family, Column column) {
    return _columnToSubColumn.get(getColumnName(family, column));
  }

  private FieldTypeDefinition getFieldTypeDefinition(String family, Column column) {
    return _fieldNameToDefMap.get(getColumnName(family, column));
  }

  @Override
  public void addColumnDefinition(String family, String columnName, String subColumnName, boolean fieldLessIndexing,
      String fieldType, Map<String, String> props) {
    String baseFieldName = family + "." + columnName;
    String fieldName;
    if (subColumnName != null) {
      if (!_fieldNameToDefMap.containsKey(baseFieldName)) {
        throw new IllegalArgumentException("Base column of [" + baseFieldName
            + "] not found, please add base before adding sub column.");
      }
      if (fieldLessIndexing) {
        throw new IllegalArgumentException("Subcolumn of [" + subColumnName + "] from base of [" + baseFieldName
            + "] cannot be added with fieldLessIndexing set to true.");
      }
      fieldName = baseFieldName + "." + subColumnName;
    } else {
      fieldName = baseFieldName;
    }
    tryToStore(fieldName, fieldLessIndexing, fieldType, props);
    Class<? extends FieldTypeDefinition> clazz = _typeMap.get(fieldType);
    if (clazz == null) {
      throw new IllegalArgumentException("FieldType of [" + fieldType + "] was not found.");
    }
    FieldTypeDefinition fieldTypeDefinition;
    try {
      fieldTypeDefinition = clazz.newInstance();
    } catch (InstantiationException e) {
      LOG.error("Unknown error trying to create a type of [{0}] from class [{1}]", e, fieldType, clazz);
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      LOG.error("Unknown error trying to create a type of [{0}] from class [{1}]", e, fieldType, clazz);
      throw new RuntimeException(e);
    }
    if (props == null) {
      fieldTypeDefinition.configure(EMPTY_MAP);
    } else {
      fieldTypeDefinition.configure(props);
    }
    fieldTypeDefinition.setFieldLessIndexing(fieldLessIndexing);
    _fieldNameToDefMap.put(fieldName, fieldTypeDefinition);
    if (subColumnName != null) {
      Set<String> subColumnNames = _columnToSubColumn.put(baseFieldName, getConcurrentSet());
      subColumnNames.add(subColumnName);
    }
  }

  protected abstract void tryToStore(String fieldName, boolean fieldLessIndexing, String fieldType,
      Map<String, String> props);

  private Set<String> getConcurrentSet() {
    return Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  }

  @Override
  public boolean isValidColumnDefinition(String family, String columnName) {
    String fieldName = getColumnName(family, columnName);
    return _fieldNameToDefMap.containsKey(fieldName);
  }

  @Override
  public Analyzer getAnalyzerForIndex(String fieldName) {
    FieldTypeDefinition fieldTypeDefinition = _fieldNameToDefMap.get(fieldName);
    if (fieldTypeDefinition == null) {
      throw new AnalyzerNotFoundException(fieldName);
    }
    return fieldTypeDefinition.getAnalyzerForIndex();
  }

  @Override
  public Analyzer getAnalyzerForQuery(String fieldName) {
    FieldTypeDefinition fieldTypeDefinition = _fieldNameToDefMap.get(fieldName);
    if (fieldTypeDefinition == null) {
      throw new AnalyzerNotFoundException(fieldName);
    }
    return fieldTypeDefinition.getAnalyzerForQuery();
  }

}
