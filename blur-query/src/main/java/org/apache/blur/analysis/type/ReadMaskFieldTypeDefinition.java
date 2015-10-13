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
import org.apache.blur.lucene.security.index.FilterAccessControlFactory.FilterAccessControlWriter;
import org.apache.blur.thrift.generated.Column;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

public class ReadMaskFieldTypeDefinition extends FieldTypeDefinition {

  private static final String INTERNAL_FIELDNAME = "_readmask_";
  private static final KeywordAnalyzer KEYWORD_ANALYZER = new KeywordAnalyzer();
  private static final String READ_MASK = "read-mask";
  private static final Collection<String> ALT_FIELD_NAMES;

  static {
    ALT_FIELD_NAMES = new HashSet<String>();
    ALT_FIELD_NAMES.add(INTERNAL_FIELDNAME);
  }

  @Override
  public String getName() {
    return READ_MASK;
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
    String name = getName(family, column.getName());
    List<Field> fields = new ArrayList<Field>();
    fields.add(new StoredField(name, column.getValue()));
    fields.add(new StringField(INTERNAL_FIELDNAME, column.getValue(), Store.YES));
    return fields;
  }

  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    return makeIterable(new StringField(INTERNAL_FIELDNAME, column.getValue(), Store.YES));
  }

  @Override
  public boolean isPostProcessingSupported() {
    return true;
  }

  @Override
  public int getPostProcessingPriority() {
    return Integer.MAX_VALUE;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterable<? extends Field> executePostProcessing(Iterable<? extends Field> fields) {
    Iterable<IndexableField> doc = FilterAccessControlWriter.processFieldMasks((Iterable<IndexableField>) fields);
    return (Iterable<? extends Field>) doc;
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
    return false;
  }

  @Override
  public String readTerm(BytesRef byteRef) {
    return byteRef.utf8ToString();
  }

  @Override
  public SortField getSortField(boolean reverse) {
    throw new RuntimeException("Sort not supported.");
  }
}
