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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.blur.thrift.generated.Column;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;

public abstract class FieldTypeDefinition {

  protected static final Collection<String> EMPTY_COLLECTION = Arrays.asList(new String[]{});
  protected boolean _fieldLessIndexing;

  /**
   * Gets the name of the field type.
   * 
   * @return the name.
   */
  public abstract String getName();

  /**
   * Gets the alternate field names for this instance. For example in the
   * PointVectorStrategy, there are two field that are created with suffixes
   * from the base field name. For example if the base field name was
   * "fam1.col1" then the two suffix names would be "fam1.col1__x" and
   * "fam1.col1__y".
   * 
   * <ul>
   * <li>fam1.col1 - This field is used for storage.</li>
   * <li>fam1.col1__x - This field is used to index the x component.</li>
   * <li>fam1.col1__y - This field is used to index the y component.</li>
   * </ul
   * 
   * @return an {@link Iterable} of {@link String}s.
   */
  public Collection<String> getAlternateFieldNames() {
    return EMPTY_COLLECTION;
  }

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
   * Gets the {@link Analyzer} for indexing this should be the same for the
   * querying unless you have a good reason.
   * 
   * @param fieldName
   *          the field name to get the analyzer, this could be the base field
   *          name or the alternative.
   * 
   * @return the {@link Analyzer}.
   */
  public abstract Analyzer getAnalyzerForIndex(String fieldName);

  /**
   * Gets the {@link Analyzer} for querying this should be the same for the
   * indexing unless you have a good reason.
   * 
   * @param fieldName
   *          the field name to get the analyzer, this could be the base field
   *          name or the alternative.
   * 
   * @return the {@link Analyzer}.
   */
  public abstract Analyzer getAnalyzerForQuery(String fieldName);

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
