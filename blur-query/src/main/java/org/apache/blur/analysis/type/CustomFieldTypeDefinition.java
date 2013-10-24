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
import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.sandbox.queries.regex.RegexQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;

public abstract class CustomFieldTypeDefinition extends FieldTypeDefinition {

  private static final String NOT_SUPPORTED = "Not supported";
  private final Analyzer _queryAnalyzer = new KeywordAnalyzer();

  /**
   * The {@link #getAnalyzerForIndex(String)} should never be called for a
   * {@link CustomFieldTypeDefinition} because this type will create the
   * {@link IndexableField} objects from the
   * {@link #getFieldsForColumn(String, Column)} method or
   * {@link #getFieldsForSubColumn(String, Column, String)} method.
   * 
   * @throws @{@link RuntimeException}.
   */
  @Override
  public final Analyzer getAnalyzerForIndex(String fieldName) {
    throw new RuntimeException(NOT_SUPPORTED);
  }

  /**
   * A {@link KeywordAnalyzer} is used to parse all the queries for
   * {@link CustomFieldTypeDefinition} types. That means that your query string
   * must contain all the parts of your query to be passed to the
   * {@link #getCustomQuery(String)} method.
   * 
   * @return {@link KeywordAnalyzer}.
   */
  @Override
  public final Analyzer getAnalyzerForQuery(String fieldName) {
    return _queryAnalyzer;
  }

  /**
   * Custom query types do not support {@link FuzzyQuery}.
   * 
   * @return false.
   */
  @Override
  public final boolean checkSupportForFuzzyQuery() {
    return false;
  }

  /**
   * Custom query types do not support {@link WildcardQuery}.
   * 
   * @return false.
   */
  @Override
  public final boolean checkSupportForWildcardQuery() {
    return false;
  }

  /**
   * Custom query types do not support {@link PrefixQuery}.
   * 
   * @return false.
   */
  @Override
  public final boolean checkSupportForPrefixQuery() {
    return false;
  }
  
  /**
   * Custom query types do not support {@link RegexQuery}.
   * 
   * @return false.
   */
  @Override
  public final boolean checkSupportForRegexQuery() {
    return false;
  }

  /**
   * Checks whether this type is numeric or not. If so you need this type to be
   * numeric please extend {@link NumericFieldTypeDefinition} instead of this
   * class.
   * 
   * @return false.
   */
  @Override
  public final boolean isNumeric() {
    return false;
  }

  /**
   * By returning true this type will create {@link Query} objects from
   * {@link #getCustomQuery(String)} method where the entire string from the
   * query parser is passed to the method. {@link #getCustomQuery(String)}
   * method (true). If you want to use the {@link #getAnalyzerForIndex(String)}
   * method to create your query, please extend {@link TextFieldTypeDefinition}
   * or {@link FieldTypeDefinition}.
   * 
   * @return true.
   */
  @Override
  public final boolean checkSupportForCustomQuery() {
    return true;
  }

}
