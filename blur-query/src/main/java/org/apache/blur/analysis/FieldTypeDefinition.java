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
import java.util.Iterator;
import java.util.Map;

import org.apache.blur.thrift.generated.Column;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.Query;

public abstract class FieldTypeDefinition {
  
  protected boolean _fieldLessIndexing;

  /**
   * Gets the name of the field type.
   * 
   * @return the name.
   */
  public abstract String getName();

  /**
   * Configures the field type.
   * 
   * @param properties
   *          the properties.
   */
  public abstract void configure(String fieldNameForThisInstance, Map<String, String> properties);

  /**
   * Gets the {@link Field}s for indexing from a single Column.
   * 
   * @param column
   *          the {@link Column}
   * @return the {@link Iterable} of fields.
   */
  public abstract Iterable<? extends Field> getFieldsForColumn(String family, Column column);

  /**
   * Gets the {@link Field}s for indexing from a single Column, but should not
   * be stored because the original value should be stored in the main
   * {@link Column}.
   * 
   * @param column
   *          the {@link Column}
   * @return the {@link Iterable} of fields.
   */
  public abstract Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName);

  /**
   * Gets the {@link FieldType} for stored version. This is the normal Column
   * for Blur.
   * 
   * @return the {@link FieldType}.
   */
  public abstract FieldType getStoredFieldType();

  /**
   * Gets the {@link FieldType} for non-stored version. This is the sub Column
   * for Blur.
   * 
   * @return the {@link FieldType}.
   */
  public abstract FieldType getNotStoredFieldType();

  /**
   * Gets the {@link Analyzer} for indexing this should be the same for the
   * querying unless you have a good reason.
   * 
   * @return the {@link Analyzer}.
   */
  public abstract Analyzer getAnalyzerForIndex();

  /**
   * Gets the {@link Analyzer} for querying this should be the same for the
   * indexing unless you have a good reason.
   * 
   * @return the {@link Analyzer}.
   */
  public abstract Analyzer getAnalyzerForQuery();

  protected Iterable<? extends Field> makeIterable(final Field field) {
    return new Iterable<Field>() {
      @Override
      public Iterator<Field> iterator() {
        return new Iterator<Field>() {

          private boolean _hasNext = true;

          @Override
          public void remove() {

          }

          @Override
          public Field next() {
            _hasNext = false;
            return field;
          }

          @Override
          public boolean hasNext() {
            return _hasNext;
          }
        };
      }
    };
  }

  protected String getName(String family, String name) {
    return family + "." + name;
  }

  protected String getName(String family, String name, String subName) {
    return family + "." + name + "." + subName;
  }

  public boolean isFieldLessIndexed() {
    return _fieldLessIndexing;
  }

  public void setFieldLessIndexing(boolean fieldLessIndexing) {
    _fieldLessIndexing = fieldLessIndexing;
  }

  public abstract boolean checkSupportForFuzzyQuery();

  public abstract boolean checkSupportForWildcardQuery();

  public abstract boolean checkSupportForPrefixQuery();

  public abstract boolean isNumeric();

  public abstract boolean checkSupportForCustomQuery();

  public Query getCustomQuery(String text) {
    throw new RuntimeException("Not supported.");
  }

}
