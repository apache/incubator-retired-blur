package org.apache.blur.analysis;

public class AnalyzerNotFoundException extends RuntimeException {

  private static final long serialVersionUID = -1669681684004161773L;

  public AnalyzerNotFoundException(String fieldName) {
    super("FieldName [" + fieldName + "] not found in the analyzer");
  }

}
