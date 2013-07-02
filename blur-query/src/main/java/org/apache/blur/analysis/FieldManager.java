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
import java.util.Map;

import org.apache.blur.thrift.generated.Record;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;

public abstract class FieldManager {

  /**
   * Gets an {@link Iterable} of fields for indexing and storing fields into
   * Lucene.
   * 
   * @param record
   * @return
   */
  public abstract Iterable<? extends Field> getFields(String rowId, Record record);

  /**
   * Adds a column definition.
   * 
   * @param family
   *          the family name, required.
   * @param columnName
   *          the column name, required.
   * @param subColumnName
   *          the sub column name, optional can be null if it's not a sub
   *          column.
   * @param fieldLessIndexing
   *          indicates whether the field should be added to the default field
   *          for the record for fieldless searching.
   * @param fieldType
   *          the field type name, required.
   * @param props
   *          the configuration properties for this column and type.
   */
  public abstract void addColumnDefinition(String family, String columnName, String subColumnName,
      boolean fieldLessIndexing, String fieldType, Map<String, String> props);

  /**
   * Gets the analyzer for the indexing process.
   * 
   * @param fieldName
   *          the Lucene field name.
   * @return {@link Analyzer}.
   */
  public abstract Analyzer getAnalyzerForIndex(String fieldName);

  /**
   * Gets the analyzer for the querying.
   * 
   * @param fieldName
   *          the Lucene field name.
   * @return {@link Analyzer}.
   */
  public abstract Analyzer getAnalyzerForQuery(String fieldName);

  /**
   * Checks if there is a valid column definition for the given family and
   * column name.
   * 
   * @param family
   *          the family name,
   * @param columnName
   *          the column name.
   * @return boolean
   */
  public abstract boolean isValidColumnDefinition(String family, String columnName);

}
