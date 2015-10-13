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

import org.apache.blur.analysis.type.MultiValuedNotAllowedException;
import org.apache.blur.thrift.generated.Column;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

public abstract class FieldTypeDefinition {

  protected static final Collection<String> EMPTY_COLLECTION = Arrays.asList(new String[] {});
  private boolean _fieldLessIndexed;
  private boolean _sortEnable;
  private String _family;
  private String _columnName;
  private String _subColumnName;
  private String _fieldType;
  private Map<String, String> _properties;
  private boolean _multiValueField;

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
  public abstract void configure(String fieldNameForThisInstance, Map<String, String> properties,
      Configuration configuration);

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

  protected String getFieldName() {
    if (_subColumnName == null) {
      return getName(_family, _columnName);
    } else {
      return getName(_family, _columnName, _subColumnName);
    }
  }

  protected String getName(String family, String name) {
    return family + "." + name;
  }

  protected String getName(String family, String name, String subName) {
    return family + "." + name + "." + subName;
  }

  public boolean isFieldLessIndexed() {
    return _fieldLessIndexed;
  }

  public void setFieldLessIndexed(boolean fieldLessIndexed) {
    _fieldLessIndexed = fieldLessIndexed;
  }

  public abstract boolean checkSupportForFuzzyQuery();

  public abstract boolean checkSupportForWildcardQuery();

  public abstract boolean checkSupportForPrefixQuery();

  public abstract boolean checkSupportForRegexQuery();

  public abstract boolean isNumeric();

  public abstract boolean checkSupportForCustomQuery();

  public abstract boolean checkSupportForSorting();

  public boolean isSortEnable() {
    return _sortEnable;
  }

  public void setSortEnable(boolean sortEnable) {
    if (sortEnable && !checkSupportForSorting()) {
      throw new RuntimeException("Field type [" + getName() + "] is not sortable.");
    }
    if (sortEnable && isMultiValueField()) {
      throw new RuntimeException("Field type [" + getName() + "] can not be sortable and multi valued.");
    }
    _sortEnable = sortEnable;
  }

  public Query getCustomQuery(String text) {
    throw new RuntimeException("Not supported.");
  }

  public String getFamily() {
    return _family;
  }

  public void setFamily(String family) {
    this._family = family;
  }

  public String getColumnName() {
    return _columnName;
  }

  public void setColumnName(String columnName) {
    this._columnName = columnName;
  }

  public String getSubColumnName() {
    return _subColumnName;
  }

  public void setSubColumnName(String subColumnName) {
    this._subColumnName = subColumnName;
  }

  public String getFieldType() {
    return _fieldType;
  }

  public void setFieldType(String fieldType) {
    this._fieldType = fieldType;
  }

  public Map<String, String> getProperties() {
    return _properties;
  }

  public void setProperties(Map<String, String> properties) {
    this._properties = properties;
  }

  public abstract SortField getSortField(boolean reverse);

  public void setMultiValueField(boolean multiValueField) {
    if (multiValueField && isSortEnable()) {
      throw new MultiValuedNotAllowedException("Field [" + getFieldName() + "] with type [" + getName()
          + "] can not be sortable and multi valued.");
    }
    _multiValueField = multiValueField;
  }

  public boolean isMultiValueField() {
    return _multiValueField;
  }

  /**
   * If true this will allow the same alternate field name(s) across instances
   * of the same {@link FieldTypeDefinition}.
   * 
   * @return booelan.
   */
  public boolean isAlternateFieldNamesSharedAcrossInstances() {
    return false;
  }

  /**
   * Method that will convert whatever internal lucene format a term was stored
   * into something that could be used as a type-ahead or term listing for a
   * given field. (by default numeric fields are not readable with a simple
   * toUtf8String())
   * 
   * @param byteRef
   *          the term that need converting into human readable form
   * @return String representation of the BytesRef or null if the term is not a
   *         real term (ie, a numeric offset for range querying)
   */
  public abstract String readTerm(BytesRef byteRef);

  public boolean isPostProcessingSupported() {
    return false;
  }

  public int getPostProcessingPriority() {
    return 0;
  }

  public Iterable<? extends Field> executePostProcessing(Iterable<? extends Field> fields) {
    throw new RuntimeException("Not Implemented.");
  }

}
