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
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.blur.thrift.generated.Record;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;

public abstract class FieldManager {

  /**
   * Gets an {@link Iterable} of fields for indexing and storing fields into
   * Lucene.
   * 
   * @param record
   * @return
   * @throws IOException 
   */
  public abstract List<Field> getFields(String rowId, Record record) throws IOException;

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
   * @return 
   * @throws IOException 
   */
  public abstract boolean addColumnDefinition(String family, String columnName, String subColumnName,
      boolean fieldLessIndexing, String fieldType, Map<String, String> props) throws IOException;

  /**
   * Gets the analyzer for the indexing process.
   * 
   * @param fieldName
   *          the Lucene field name.
   * @return {@link Analyzer}.
   * @throws IOException 
   */
  public abstract Analyzer getAnalyzerForIndex(String fieldName) throws IOException;

  /**
   * Gets the analyzer for the querying.
   * 
   * @param fieldName
   *          the Lucene field name.
   * @return {@link Analyzer}.
   * @throws IOException 
   */
  public abstract Analyzer getAnalyzerForQuery(String fieldName) throws IOException;

  /**
   * Checks if there is a valid column definition for the given family and
   * column name.
   * 
   * @param family
   *          the family name,
   * @param columnName
   *          the column name.
   * @return boolean
   * @throws IOException 
   */
  public abstract boolean isValidColumnDefinition(String family, String columnName) throws IOException;

  /**
   * Gets an {@link Analyzer} for quering.
   * 
   * @return the analyzer
   */
  public abstract Analyzer getAnalyzerForQuery();

  /**
   * Checks if this field supports fuzzy queries.
   * 
   * @param field
   *          the field name.
   * @return boolean
   * @throws IOException 
   */
  public abstract boolean checkSupportForFuzzyQuery(String field) throws IOException;

  /**
   * Checks if this field supports prefix queries.
   * 
   * @param field
   *          the field name.
   * @return boolean
   * @throws IOException 
   */
  public abstract boolean checkSupportForPrefixQuery(String field) throws IOException;

  /**
   * Checks if this field supports wildcard queries.
   * 
   * @param field
   *          the field name.
   * @return boolean
   * @throws IOException 
   */
  public abstract boolean checkSupportForWildcardQuery(String field) throws IOException;

  /**
   * Gets a range query, if the method returns null the default term range query
   * is used in the parser. This is used to create numeric range queries.
   * 
   * @param field
   *          the field name.
   * @param part1
   *          the first part.
   * @param part2
   *          the second part.
   * @param startInclusive
   *          if the start is inclusive.
   * @param endInclusive
   *          if the end is inclusive.
   * @return the new range query or null.
   * @throws IOException 
   */
  public abstract Query getNewRangeQuery(String field, String part1, String part2, boolean startInclusive,
      boolean endInclusive) throws IOException;

  /**
   * This method should return a query (probably a range query) for numeric
   * fields and null for other fields.
   * 
   * @param field
   *          the field name.
   * @param text
   *          the text for the term.
   * @return the query or null.
   * @throws IOException 
   */
  public abstract Query getTermQueryIfNumeric(String field, String text) throws IOException;

  /**
   * Gets the {@link FieldTypeDefinition} for the given field.
   * 
   * @param field
   *          the field name.
   * @return the {@link FieldTypeDefinition} or null if missing.
   * @throws IOException 
   */
  public abstract FieldTypeDefinition getFieldTypeDefinition(String field) throws IOException;

  public abstract boolean isFieldLessIndexed(String name) throws IOException;

  public abstract Analyzer getAnalyzerForIndex();
  
  public abstract String getFieldLessFieldName();

}
