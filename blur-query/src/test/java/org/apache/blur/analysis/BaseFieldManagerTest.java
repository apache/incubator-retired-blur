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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.analysis.type.TextFieldTypeDefinition;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.junit.Test;

public class BaseFieldManagerTest {

  public static final String _fieldLessField = BlurConstants.SUPER;

  @Test
  public void testFieldManager() throws IOException {
    FieldManager memoryFieldManager = newFieldManager(true);
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, true, "text", false, false, null);

    Record record = new Record();
    record.setFamily("fam1");
    record.setRecordId("1213");
    record.addToColumns(new Column("col1", "value1"));

    List<Field> fields = getFields("fam1", "1", "1213", newFieldsNoStore(BlurConstants.FIELDS, "fam1.col1"),
        newTextField("fam1.col1", "value1"), newTextFieldNoStore(_fieldLessField, "value1"));

    int c = 0;
    for (Field field : memoryFieldManager.getFields("1", record)) {
      assertFieldEquals(fields.get(c++), field);
    }
  }

  private Field newFieldsNoStore(String name, String value) {
    return new StringField(name, value, Store.NO);
  }

  @Test
  public void testFieldManagerWithNullFamily() throws IOException {
    FieldManager memoryFieldManager = newFieldManager(true);
    memoryFieldManager.addColumnDefinition(null, "col1", null, true, "text", false, false, null);

    Record record = new Record();
    record.setRecordId("1213");
    record.addToColumns(new Column("col1", "value1"));

    List<Field> fields = getFields(null, "1", "1213", newFieldsNoStore(BlurConstants.FIELDS, "_default_.col1"),
        newTextField(memoryFieldManager.resolveField("col1"), "value1"), newTextFieldNoStore(_fieldLessField, "value1"));

    int c = 0;
    for (Field field : memoryFieldManager.getFields("1", record)) {
      assertFieldEquals(fields.get(c++), field);
    }
  }

  @Test
  public void testFieldManagerMultipleColumnsSameName() throws IOException {
    FieldManager memoryFieldManager = newFieldManager(true);
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, false, "text", false, true, null);

    Record record = new Record();
    record.setFamily("fam1");
    record.setRecordId("1213");
    record.addToColumns(new Column("col1", "value1"));
    record.addToColumns(new Column("col1", "value2"));

    List<Field> fields = getFields("fam1", "1", "1213", newFieldsNoStore(BlurConstants.FIELDS, "fam1.col1"),
        newFieldsNoStore(BlurConstants.FIELDS, "fam1.col1"), newTextField("fam1.col1", "value1"),
        newTextField("fam1.col1", "value2"));

    int c = 0;
    for (Field field : memoryFieldManager.getFields("1", record)) {
      // assertFieldEquals(fields.get(c++), field);
      System.out.println("Should be [" + fields.get(c++) + "] was [" + field + "]");
    }

  }

  @Test
  public void testFieldManagerMultipleColumnsSameNameWhenNotAllowed() throws IOException {
    FieldManager memoryFieldManager = newFieldManager(true);
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, false, "text", false, false, null);

    Record record = new Record();
    record.setFamily("fam1");
    record.setRecordId("1213");
    record.addToColumns(new Column("col1", "value1"));
    record.addToColumns(new Column("col1", "value2"));

    try {
      memoryFieldManager.getFields("1", record);
      fail();
    } catch (IOException ex) {
      assertEquals("Field [fam1.col1] does not allow more than one column value.", ex.getMessage());
    }
  }

  @Test
  public void testFieldManagerMultipleColumnsDifferentNames() throws IOException {
    FieldManager memoryFieldManager = newFieldManager(true);
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, false, "text", false, false, null);
    memoryFieldManager.addColumnDefinition("fam1", "col2", null, true, "text", false, false, null);

    Record record = new Record();
    record.setFamily("fam1");
    record.setRecordId("1213");
    record.addToColumns(new Column("col1", "value1"));
    record.addToColumns(new Column("col2", "value2"));

    List<Field> fields = getFields("fam1", "1", "1213", newFieldsNoStore(BlurConstants.FIELDS, "fam1.col1"),
        newFieldsNoStore(BlurConstants.FIELDS, "fam1.col2"), newTextField("fam1.col1", "value1"),
        newTextField("fam1.col2", "value2"), newTextFieldNoStore(BlurConstants.SUPER, "value2"));

    int c = 0;
    List<Field> fields2 = memoryFieldManager.getFields("1", record);
    for (Field field : fields2) {
      assertFieldEquals(fields.get(c++), field);
    }
  }

  @Test
  public void testFieldManagerMultipleColumnsDifferentNamesDifferentFamilies() throws IOException {
    FieldManager memoryFieldManager = newFieldManager(true);
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, false, "text", false, false, null);
    memoryFieldManager.addColumnDefinition("fam2", "col2", null, false, "text", false, false, null);

    Record record1 = new Record();
    record1.setFamily("fam1");
    record1.setRecordId("1213");
    record1.addToColumns(new Column("col1", "value1"));

    List<Field> fields1 = getFields("fam1", "1", "1213", newFieldsNoStore(BlurConstants.FIELDS, "fam1.col1"),
        newTextField("fam1.col1", "value1"));
    int c1 = 0;
    for (Field field : memoryFieldManager.getFields("1", record1)) {
      assertFieldEquals(fields1.get(c1++), field);
    }

    Record record2 = new Record();
    record2.setFamily("fam2");
    record2.setRecordId("1213");
    record2.addToColumns(new Column("col2", "value1"));

    List<Field> fields2 = getFields("fam2", "1", "1213", newFieldsNoStore(BlurConstants.FIELDS, "fam2.col2"),
        newTextField("fam2.col2", "value1"));
    int c2 = 0;
    for (Field field : memoryFieldManager.getFields("1", record2)) {
      assertFieldEquals(fields2.get(c2++), field);
    }
  }

  @Test
  public void testFieldManagerMultipleColumnsDifferentNamesNullFamilies() throws IOException {
    FieldManager memoryFieldManager = newFieldManager(true);
    memoryFieldManager.addColumnDefinition(null, "col1", null, false, "text", false, false, null);
    memoryFieldManager.addColumnDefinition(null, "col2", null, false, "text", false, false, null);

    Record record1 = new Record();
    record1.setRecordId("1213");
    record1.addToColumns(new Column("col1", "value1"));

    List<Field> fields1 = getFields(null, "1", "1213",
        newFieldsNoStore(BlurConstants.FIELDS, BlurConstants.DEFAULT_FAMILY + ".col1"),
        newTextField(memoryFieldManager.resolveField("col1"), "value1"));
    int c1 = 0;
    for (Field field : memoryFieldManager.getFields("1", record1)) {
      assertFieldEquals(fields1.get(c1++), field);
    }

    Record record2 = new Record();
    record2.setRecordId("1213");
    record2.addToColumns(new Column("col2", "value1"));

    List<Field> fields2 = getFields(null, "1", "1213",
        newFieldsNoStore(BlurConstants.FIELDS, BlurConstants.DEFAULT_FAMILY + ".col2"),
        newTextField(memoryFieldManager.resolveField("col2"), "value1"));
    int c2 = 0;
    for (Field field : memoryFieldManager.getFields("1", record2)) {
      assertFieldEquals(fields2.get(c2++), field);
    }
  }

  @Test
  public void testFieldManagerSubNameWithMainColumnNameNoParent() throws IOException {
    FieldManager memoryFieldManager = newFieldManager(true);
    try {
      memoryFieldManager.addColumnDefinition("fam1", "col1", "sub1", false, "text", false, false, null);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testFieldManagerSubNameWithMainColumnNameNoFieldLess() throws IOException {
    FieldManager memoryFieldManager = newFieldManager(true);
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, false, "text", false, false, null);
    try {
      memoryFieldManager.addColumnDefinition("fam1", "col1", "sub1", true, "text", false, false, null);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testFieldManagerSubName() throws IOException {
    FieldManager memoryFieldManager = newFieldManager(true);
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, false, "text", false, false, null);
    memoryFieldManager.addColumnDefinition("fam1", "col1", "sub1", false, "text", false, false, null);

    Record record = new Record();
    record.setRecordId("1213");
    record.setFamily("fam1");
    record.addToColumns(new Column("col1", "value1"));

    List<Field> fields = getFields("fam1", "1", "1213", newFieldsNoStore(BlurConstants.FIELDS, "fam1.col1"),
        newTextField("fam1.col1", "value1"), newTextFieldNoStore("fam1.col1.sub1", "value1"));
    int c = 0;
    for (Field field : memoryFieldManager.getFields("1", record)) {
      assertFieldEquals(fields.get(c++), field);
    }
  }

  private List<Field> getFields(String family, String rowId, String recordId, Field... fields) {
    List<Field> fieldLst = new ArrayList<Field>();
    if (family != null) {
      fieldLst.add(new Field(BlurConstants.FAMILY, family, BaseFieldManager.ID_TYPE));
    } else {
      fieldLst.add(new Field(BlurConstants.FAMILY, BlurConstants.DEFAULT_FAMILY, BaseFieldManager.ID_TYPE));
    }
    fieldLst.add(new Field(BlurConstants.ROW_ID, rowId, BaseFieldManager.ID_TYPE));
    fieldLst.add(new Field(BlurConstants.RECORD_ID, recordId, BaseFieldManager.ID_TYPE));
    for (Field field : fields) {
      fieldLst.add(field);
    }
    return fieldLst;
  }

  private Field newTextField(String name, String value) {
    return new Field(name, value, TextFieldTypeDefinition.TYPE_STORED);
  }

  private Field newTextFieldNoStore(String name, String value) {
    return new Field(name, value, TextFieldTypeDefinition.TYPE_NOT_STORED);
  }

  private void assertFieldEquals(Field expected, Field actual) {
    assertEquals("Names did not match Expected [" + expected + "] Actual [" + actual + "]", expected.name(),
        actual.name());
    assertEquals("Values did not match Expected [" + expected + "] Actual [" + actual + "]", expected.stringValue(),
        actual.stringValue());
    assertEquals("FileTypes did not match Expected [" + expected + "] Actual [" + actual + "]", expected.fieldType()
        .toString(), actual.fieldType().toString());
  }

  protected FieldManager newFieldManager(boolean create) throws IOException {
    return new BaseFieldManager(_fieldLessField, new KeywordAnalyzer(), new Configuration()) {
      @Override
      protected boolean tryToStore(FieldTypeDefinition fieldTypeDefinition, String fieldName) {
        return true;
      }

      @Override
      protected void tryToLoad(String field) {

      }

      @Override
      protected List<String> getFieldNamesToLoad() throws IOException {
        return new ArrayList<String>();
      }
    };
  }

}
