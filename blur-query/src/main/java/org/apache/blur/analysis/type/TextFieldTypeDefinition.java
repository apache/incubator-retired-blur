package org.apache.blur.analysis.type;

import java.util.Map;

import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.analysis.NoStopWordStandardAnalyzer;
import org.apache.blur.thrift.generated.Column;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;

public class TextFieldTypeDefinition extends FieldTypeDefinition {

  private static final String TEXT = "text";
  private static final FieldType TYPE_NOT_STORED;
  private static final FieldType TYPE_STORED;

  static {
    TYPE_STORED = new FieldType(TextField.TYPE_STORED);
    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.freeze();

    TYPE_NOT_STORED = new FieldType(TextField.TYPE_NOT_STORED);
    TYPE_NOT_STORED.setOmitNorms(true);
    TYPE_NOT_STORED.freeze();
  }

  @Override
  public String getName() {
    return TEXT;
  }

  @Override
  public void configure(Map<String, String> properties) {

  }

  @Override
  public Iterable<? extends Field> getFieldsForColumn(String family, Column column) {
    String name = getName(family, column.getName());
    Field field = new Field(name, column.getValue(), getStoredFieldType());
    return makeIterable(field);
  }

  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    String name = getName(family, column.getName(), subName);
    Field field = new Field(name, column.getValue(), getNotStoredFieldType());
    return makeIterable(field);
  }

  @Override
  public FieldType getStoredFieldType() {
    return TYPE_STORED;
  }

  @Override
  public FieldType getNotStoredFieldType() {
    return TYPE_NOT_STORED;
  }

  @Override
  public Analyzer getAnalyzerForIndex() {
    return new NoStopWordStandardAnalyzer();
  }

  @Override
  public Analyzer getAnalyzerForQuery() {
    return new NoStopWordStandardAnalyzer();
  }

}
