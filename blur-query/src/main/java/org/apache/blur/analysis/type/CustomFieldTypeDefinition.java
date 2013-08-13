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
import org.apache.lucene.document.FieldType;

public abstract class CustomFieldTypeDefinition extends FieldTypeDefinition {

  private static final String NOT_SUPPORTED = "Not supported";
  private final Analyzer _queryAnalyzer = new KeywordAnalyzer();

  @Override
  public final FieldType getStoredFieldType() {
    throw new RuntimeException(NOT_SUPPORTED);
  }

  @Override
  public final FieldType getNotStoredFieldType() {
    throw new RuntimeException(NOT_SUPPORTED);
  }

  @Override
  public final Analyzer getAnalyzerForIndex() {
    throw new RuntimeException(NOT_SUPPORTED);
  }

  @Override
  public final Analyzer getAnalyzerForQuery() {
    return _queryAnalyzer;
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
  public final boolean isNumeric() {
    return false;
  }

  @Override
  public final boolean checkSupportForCustomQuery() {
    return true;
  }
  
}
