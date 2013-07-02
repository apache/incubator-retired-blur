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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;

public class LightBlurAnalyzer extends AnalyzerWrapper {

  private final FieldManager _fieldManager;
  private final boolean _usedForIndexing;

  public LightBlurAnalyzer(boolean usedForIndexing, FieldManager fieldManager) {
    _usedForIndexing = usedForIndexing;
    _fieldManager = fieldManager;
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    if (_usedForIndexing) {
      return _fieldManager.getAnalyzerForIndex(fieldName);
    } else {
      return _fieldManager.getAnalyzerForQuery(fieldName);
    }
  }

  @Override
  protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
    return components;
  }

}
