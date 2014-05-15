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

import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;

public abstract class NumericFieldTypeDefinition extends FieldTypeDefinition {

  protected static final String NUMERIC_PRECISION_STEP = "numericPrecisionStep";

  protected static final String MAX = "max";
  protected static final String MIN = "min";

  protected int _precisionStep = NumericUtils.PRECISION_STEP_DEFAULT;

  @Override
  public final Analyzer getAnalyzerForIndex(String fieldName) {
    // shouldn't be used ever
    return new KeywordAnalyzer();
  }

  @Override
  public final Analyzer getAnalyzerForQuery(String fieldName) {
    return new KeywordAnalyzer();
  }

  @Override
  public final boolean checkSupportForFuzzyQuery() {
    return false;
  }

  @Override
  public final boolean checkSupportForWildcardQuery() {
    return false;
  }

  @Override
  public final boolean checkSupportForPrefixQuery() {
    return false;
  }

  @Override
  public final boolean checkSupportForRegexQuery() {
    return false;
  }

  @Override
  public final boolean isNumeric() {
    return true;
  }

  @Override
  public final boolean checkSupportForCustomQuery() {
    return false;
  }

  @Override
  public boolean checkSupportForSorting() {
    return true;
  }
  
  protected Iterable<? extends Field> addSort(String name, long value, Field field) {
    List<Field> list = new ArrayList<Field>();
    list.add(field);
    list.add(new NumericDocValuesField(name, value));
    return list;
  }

  public abstract Query getNewRangeQuery(String field, String part1, String part2, boolean startInclusive,
      boolean endInclusive);

}
