package org.apache.blur.analysis.type;

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
import java.util.List;
import java.util.Map;

import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.thrift.generated.Column;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;

public class StringFieldTypeDefinition extends FieldTypeDefinition {

  private static final KeywordAnalyzer KEYWORD_ANALYZER = new KeywordAnalyzer();
  public static final String NAME = "string";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void configure(String fieldNameForThisInstance, Map<String, String> properties, Configuration configuration) {
  }

  @Override
  public Iterable<? extends Field> getFieldsForColumn(String family, Column column) {
    String name = getName(family, column.getName());
    Field field = new Field(name, column.getValue(), StringField.TYPE_STORED);
    if (isSortEnable()) {
      return addSort(column, name, field);
    }
    return makeIterable(field);
  }

  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    String name = getName(family, column.getName(), subName);
    Field field = new Field(name, column.getValue(), StringField.TYPE_NOT_STORED);
    if (isSortEnable()) {
      return addSort(column, name, field);
    }
    return makeIterable(field);
  }

  private Iterable<? extends Field> addSort(Column column, String name, Field field) {
    List<Field> list = new ArrayList<Field>();
    list.add(field);
    list.add(new SortedDocValuesField(name, new BytesRef(column.getValue())));
    return list;
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
  public boolean isNumeric() {
    return false;
  }

  @Override
  public boolean checkSupportForCustomQuery() {
    return false;
  }

  @Override
  public boolean checkSupportForRegexQuery() {
    return true;
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
}
