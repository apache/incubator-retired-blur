package com.nearinfinity.blur.analysis;

import java.io.Reader;

import org.apache.lucene.analysis.KeywordTokenizer;
import org.apache.lucene.analysis.ReusableAnalyzerBase;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.util.NumericUtils;

public class LongAnalyzer extends ReusableAnalyzerBase implements FieldConverter {

  private static final String TYPE = "long";

  private int precisionStepDefault = NumericUtils.PRECISION_STEP_DEFAULT;

  public LongAnalyzer(String typeStr) {
    if (typeStr.startsWith(TYPE)) {
      int index = typeStr.indexOf(',');
      if (index > 0) {
        String s = typeStr.substring(index + 1);
        try {
          precisionStepDefault = Integer.parseInt(s);
        } catch (NumberFormatException e) {
          throw new RuntimeException("Can not parser [" + s + "] into an integer for the precisionStepDefault.");
        }
      }
    } else {
      throw new RuntimeException("Long type can not parser [" + typeStr + "]");
    }
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    return new TokenStreamComponents(new KeywordTokenizer(reader));
  }

  @Override
  public Fieldable convert(Fieldable fieldable) {
    long value = Long.parseLong(fieldable.stringValue().trim());
    NumericField field = new NumericField(fieldable.name(), precisionStepDefault, fieldable.isStored() ? Store.YES : Store.NO, true);
    field.setLongValue(value);
    return field;
  }

}
