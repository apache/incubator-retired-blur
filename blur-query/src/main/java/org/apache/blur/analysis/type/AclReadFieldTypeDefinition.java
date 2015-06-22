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
package org.apache.blur.analysis.type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.lucene.security.document.DocumentVisiblityField;
import org.apache.blur.thrift.generated.Column;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;

public class AclReadFieldTypeDefinition extends FieldTypeDefinition {

  private static final String INTERNAL_FIELDNAME = "_read_";
  private static final KeywordAnalyzer KEYWORD_ANALYZER = new KeywordAnalyzer();
  private static final String ACL_READ = "acl-read";
  private static final Collection<String> ALT_FIELD_NAMES;

  static {
    ALT_FIELD_NAMES = new HashSet<String>();
    ALT_FIELD_NAMES.add(INTERNAL_FIELDNAME);
  }

  @Override
  public String getName() {
    return ACL_READ;
  }

  @Override
  public void configure(String fieldNameForThisInstance, Map<String, String> properties, Configuration configuration) {

  }

  @Override
  public Collection<String> getAlternateFieldNames() {
    return ALT_FIELD_NAMES;
  }

  @Override
  public boolean isAlternateFieldNamesSharedAcrossInstances() {
    return true;
  }

  @Override
  public Iterable<? extends Field> getFieldsForColumn(String family, Column column) {
    String fieldName = getFieldName();
    String value = column.getValue();
    List<Field> fields = new ArrayList<Field>();
    fields.add(new DocumentVisiblityField(INTERNAL_FIELDNAME, value, Store.NO));
    fields.add(new StoredField(fieldName, value));
    if (isSortEnable()) {
      fields.add(new SortedDocValuesField(fieldName, new BytesRef(value)));
    }
    return fields;
  }

  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    String fieldName = getFieldName();
    String value = column.getValue();
    List<Field> fields = new ArrayList<Field>();
    fields.add(new DocumentVisiblityField(INTERNAL_FIELDNAME, value, Store.NO));
    if (isSortEnable()) {
      fields.add(new SortedDocValuesField(fieldName, new BytesRef(value)));
    }
    return fields;
  }

  @Override
  public Analyzer getAnalyzerForIndex(String fieldName) {
    // shouldn't be used ever
    return KEYWORD_ANALYZER;
  }

  @Override
  public Analyzer getAnalyzerForQuery(String fieldName) {
    return KEYWORD_ANALYZER;
  }

  @Override
  public boolean checkSupportForFuzzyQuery() {
    return true;
  }

  @Override
  public boolean checkSupportForWildcardQuery() {
    return true;
  }

  @Override
  public boolean checkSupportForPrefixQuery() {
    return true;
  }

  @Override
  public boolean checkSupportForRegexQuery() {
    return true;
  }

  @Override
  public boolean isNumeric() {
    return false;
  }

  @Override
  public boolean checkSupportForCustomQuery() {
    return false;
  }

  @Override
  public boolean checkSupportForSorting() {
    return true;
  }

  @Override
  public SortField getSortField(boolean reverse) {
    if (reverse) {
      return new SortField(getFieldName(), Type.STRING, reverse);
    }
    return new SortField(getFieldName(), Type.STRING);
  }

  @Override
  public String readTerm(BytesRef byteRef) {
    return byteRef.utf8ToString();
  }
}
