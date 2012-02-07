package com.nearinfinity.blur.utils;

import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Record;

public class BlurThriftRecord extends Record implements ReaderBlurRecord {

  private static final long serialVersionUID = 1447192115360284850L;

  @Override
  public void addColumn(String name, String value) {
    addToColumns(new Column(name, value));
  }

  @Override
  public void setRecordIdStr(String value) {
    setRecordId(value);
  }

  @Override
  public void setFamilyStr(String family) {
    setFamily(family);
  }

  @Override
  public void setRowIdStr(String rowId) {
    setRowIdStr(rowId);
  }

}
