package com.nearinfinity.blur.utils;

public interface ReaderBlurRecord {

  void setRecordIdStr(String value);

  void addColumn(String name, String value);

  void setFamilyStr(String family);

  void setRowIdStr(String rowId);

}
