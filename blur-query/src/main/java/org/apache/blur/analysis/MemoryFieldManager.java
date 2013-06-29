package org.apache.blur.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;

public class MemoryFieldManager extends FieldManager {

  private static final Log LOG = LogFactory.getLog(MemoryFieldManager.class);

  private Map<String, FieldTypeDefinition> _fieldNameToDefMap = new ConcurrentHashMap<String, FieldTypeDefinition>();
  private Map<String, Class<? extends FieldTypeDefinition>> _typeMap = new ConcurrentHashMap<String, Class<? extends FieldTypeDefinition>>();

  @Override
  public Iterable<? extends Field> getFields(Record record) {
    List<Field> fields = new ArrayList<Field>();
    String family = record.getFamily();
    List<Column> columns = record.getColumns();
    addDefaultFields(fields,record);
    for (Column column : columns) {
      getAndAddFields(fields, family, column, getFieldTypeDefinition(column));
      for (String subName : getSubColumns(column)) {
        getAndAddFields(fields, family, column, getFieldTypeDefinition(column, subName));
      }
    }
    return fields;
  }

  private void addDefaultFields(List<Field> fields, Record record) {
    //add family
    //record id fields
    
  }

  private void getAndAddFields(List<Field> fields, String family, Column column, FieldTypeDefinition fieldTypeDefinition) {
    for (Field field : fieldTypeDefinition.getFieldsForColumn(family, column)) {
      fields.add(field);
    }
  }

  private FieldTypeDefinition getFieldTypeDefinition(Column column, String subName) {
    // TODO Auto-generated method stub
    return null;
  }

  private List<String> getSubColumns(Column column) {
    // TODO Auto-generated method stub
    return null;
  }

  private FieldTypeDefinition getFieldTypeDefinition(Column column) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addColumnDefinition(String family, String columnName, String subColumnName, String fieldType,
      Map<String, String> props) {
    String baseFieldName = family + "." + columnName;
    String fieldName;
    if (subColumnName != null) {
      if (!_fieldNameToDefMap.containsKey(baseFieldName)) {
        throw new IllegalArgumentException("Base column of [" + baseFieldName
            + "] not found, please add base before adding sub column.");
      }
      fieldName = baseFieldName + "." + subColumnName;
    } else {
      fieldName = baseFieldName;
    }
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
    fieldTypeDefinition.configure(props);
    _fieldNameToDefMap.put(fieldName, fieldTypeDefinition);
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
