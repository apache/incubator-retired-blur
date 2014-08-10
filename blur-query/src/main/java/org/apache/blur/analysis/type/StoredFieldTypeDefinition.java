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
import java.util.Arrays;
import java.util.Map;

import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.thrift.generated.Column;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

public class StoredFieldTypeDefinition extends FieldTypeDefinition {

  public static final String NAME = "stored";
  private static final Iterable<? extends Field> EMPTY = Arrays.asList();

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
    return makeIterable(new StoredField(name, column.getValue()));
  }

  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    return EMPTY;
  }

  @Override
  public Analyzer getAnalyzerForIndex(String fieldName) {
    // shouldn't be used ever
    return new KeywordAnalyzer();
  }

  @Override
  public Analyzer getAnalyzerForQuery(String fieldName) {
    return new KeywordAnalyzer();
  }

  @Override
  public boolean checkSupportForFuzzyQuery() {
    return false;
  }

  @Override
  public boolean checkSupportForWildcardQuery() {
    return false;
  }

  @Override
  public boolean checkSupportForPrefixQuery() {
    return false;
  }

  @Override
  public boolean checkSupportForRegexQuery() {
    return false;
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
  public SortField getSortField(boolean reverse) {
    throw new RuntimeException("Sort not supported.");
  }
  
  @Override
  public String readTerm(BytesRef byteRef) {
	return byteRef.utf8ToString();
  }
}
