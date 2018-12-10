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
package org.apache.blur.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;

public class ThriftFieldManager extends BaseFieldManager {

  private final Iface _client;
  private final String _table;

  public ThriftFieldManager(String fieldLessField, Analyzer defaultAnalyzerForQuerying, boolean strict,
      String defaultMissingFieldType, boolean defaultMissingFieldLessIndexing,
      Map<String, String> defaultMissingFieldProps, Configuration configuration, Iface client, String table)
      throws IOException {
    super(fieldLessField, defaultAnalyzerForQuerying, strict, defaultMissingFieldType, defaultMissingFieldLessIndexing,
        defaultMissingFieldProps, configuration);
    _client = client;
    _table = table;
  }

  @Override
  protected boolean tryToStore(FieldTypeDefinition fieldTypeDefinition, String fieldName) throws IOException {
    ColumnDefinition columnDefinition = new ColumnDefinition();
    columnDefinition.setColumnName(fieldTypeDefinition.getColumnName());
    columnDefinition.setFamily(fieldTypeDefinition.getFamily());
    columnDefinition.setFieldLessIndexed(fieldTypeDefinition.isFieldLessIndexed());
    columnDefinition.setFieldType(fieldTypeDefinition.getFieldType());
    columnDefinition.setProperties(fieldTypeDefinition.getProperties());
    columnDefinition.setSortable(fieldTypeDefinition.isSortEnable());
    columnDefinition.setSubColumnName(fieldTypeDefinition.getSubColumnName());
    columnDefinition.setMultiValueField(fieldTypeDefinition.isMultiValueField());

    try {
      return _client.addColumnDefinition(_table, columnDefinition);
    } catch (BlurException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void tryToLoad(String fieldName) throws IOException {
    try {
      Schema schema = _client.schema(_table);
      int indexOf = fieldName.indexOf('.');
      if (indexOf < 0) {
        throw new IOException("Field [" + fieldName + "] not a valid name.");
      }
      Map<String, Map<String, ColumnDefinition>> families = schema.getFamilies();
      for (Entry<String, Map<String, ColumnDefinition>> familyEntry : families.entrySet()) {
        Map<String, ColumnDefinition> columnDefMap = familyEntry.getValue();
        for (Entry<String, ColumnDefinition> columnDefEntry : columnDefMap.entrySet()) {
          ColumnDefinition columnDefinition = columnDefEntry.getValue();
          String field = getFieldName(columnDefinition);
          if (field.equals(fieldName)) {
            boolean fieldLessIndexing = columnDefinition.isFieldLessIndexed();
            boolean sortenabled = columnDefinition.isSortable();
            boolean multiValueField = columnDefinition.isMultiValueField();
            Map<String, String> props = columnDefinition.getProperties();

            String fieldType = columnDefinition.getFieldType();
            FieldTypeDefinition fieldTypeDefinition = newFieldTypeDefinition(fieldName, fieldLessIndexing, fieldType,
                sortenabled, multiValueField, props);
            fieldTypeDefinition.setFamily(columnDefinition.getFamily());
            fieldTypeDefinition.setColumnName(columnDefinition.getColumnName());
            fieldTypeDefinition.setSubColumnName(columnDefinition.getSubColumnName());
            fieldTypeDefinition.setFieldLessIndexed(fieldLessIndexing);
            fieldTypeDefinition.setFieldType(fieldType);
            fieldTypeDefinition.setProperties(props);
            registerFieldTypeDefinition(fieldName, fieldTypeDefinition);
          }
        }
      }
    } catch (BlurException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  private String getFieldName(ColumnDefinition columnDefinition) {
    String family = columnDefinition.getFamily();
    String columnName = columnDefinition.getColumnName();
    String subColumnName = columnDefinition.getSubColumnName();
    if (subColumnName != null) {
      return family + "." + columnName + "." + subColumnName;
    }
    return family + "." + columnName;
  }

  @Override
  protected List<String> getFieldNamesToLoad() throws IOException {
    try {
      List<String> result = new ArrayList<String>();
      Schema schema = _client.schema(_table);
      Map<String, Map<String, ColumnDefinition>> families = schema.getFamilies();
      for (Entry<String, Map<String, ColumnDefinition>> familyEntry : families.entrySet()) {
        Map<String, ColumnDefinition> columnDefMap = familyEntry.getValue();
        for (Entry<String, ColumnDefinition> columnDefEntry : columnDefMap.entrySet()) {
          ColumnDefinition columnDefinition = columnDefEntry.getValue();
          result.add(getFieldName(columnDefinition));
        }
      }
      return result;
    } catch (BlurException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

}
