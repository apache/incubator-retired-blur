package org.apache.blur.analysis;

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
