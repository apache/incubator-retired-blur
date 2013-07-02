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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.blur.analysis.type.TextFieldTypeDefinition;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.junit.Test;

public class BaseFiledManagerTest {

  @Test
  public void testFiledManager() {
    BaseFieldManager memoryFieldManager = newBaseFieldManager();
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, true, "text", null);

    Record record = new Record();
    record.setFamily("fam1");
    record.setRecordId("1213");
    record.addToColumns(new Column("col1", "value1"));

    List<Field> fields = getFields("fam1", "1", "1213", newTextField("fam1.col1", "value1"),
        newTextFieldNoStore(BlurConstants.SUPER, "value1"));

    int c = 0;
    for (Field field : memoryFieldManager.getFields("1", record)) {
      assertFieldEquals(fields.get(c++), field);
    }
  }

  @Test
  public void testFiledManagerMultipleColumnsSameName() {
    BaseFieldManager memoryFieldManager = newBaseFieldManager();
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, false, "text", null);

    Record record = new Record();
    record.setFamily("fam1");
    record.setRecordId("1213");
    record.addToColumns(new Column("col1", "value1"));
    record.addToColumns(new Column("col1", "value2"));

    List<Field> fields = getFields("fam1", "1", "1213", newTextField("fam1.col1", "value1"),
        newTextField("fam1.col1", "value2"));

    int c = 0;
    for (Field field : memoryFieldManager.getFields("1", record)) {
      assertFieldEquals(fields.get(c++), field);
    }

  }

  @Test
  public void testFiledManagerMultipleColumnsDifferentNames() {
    BaseFieldManager memoryFieldManager = newBaseFieldManager();
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, false, "text", null);
    memoryFieldManager.addColumnDefinition("fam1", "col2", null, true, "text", null);

    Record record = new Record();
    record.setFamily("fam1");
    record.setRecordId("1213");
    record.addToColumns(new Column("col1", "value1"));
    record.addToColumns(new Column("col2", "value2"));

    List<Field> fields = getFields("fam1", "1", "1213", newTextField("fam1.col1", "value1"),
        newTextField("fam1.col2", "value2"), newTextFieldNoStore(BlurConstants.SUPER, "value2"));

    int c = 0;
    for (Field field : memoryFieldManager.getFields("1", record)) {
      assertFieldEquals(fields.get(c++), field);
    }
  }

  @Test
  public void testFiledManagerMultipleColumnsDifferentNamesDifferentFamilies() {
    BaseFieldManager memoryFieldManager = newBaseFieldManager();
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, false, "text", null);
    memoryFieldManager.addColumnDefinition("fam2", "col2", null, false, "text", null);

    Record record1 = new Record();
    record1.setFamily("fam1");
    record1.setRecordId("1213");
    record1.addToColumns(new Column("col1", "value1"));

    List<Field> fields1 = getFields("fam1", "1", "1213", newTextField("fam1.col1", "value1"));
    int c1 = 0;
    for (Field field : memoryFieldManager.getFields("1", record1)) {
      assertFieldEquals(fields1.get(c1++), field);
    }

    Record record2 = new Record();
    record2.setFamily("fam2");
    record2.setRecordId("1213");
    record2.addToColumns(new Column("col2", "value1"));

    List<Field> fields2 = getFields("fam2", "1", "1213", newTextField("fam2.col2", "value1"));
    int c2 = 0;
    for (Field field : memoryFieldManager.getFields("1", record2)) {
      assertFieldEquals(fields2.get(c2++), field);
    }
  }

  @Test
  public void testFiledManagerSubNameWithMainColumnNameNoParent() {
    BaseFieldManager memoryFieldManager = newBaseFieldManager();
    try {
      memoryFieldManager.addColumnDefinition("fam1", "col1", "sub1", false, "text", null);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testFiledManagerSubNameWithMainColumnNameNoFieldLess() {
    BaseFieldManager memoryFieldManager = newBaseFieldManager();
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, false, "text", null);
    try {
      memoryFieldManager.addColumnDefinition("fam1", "col1", "sub1", true, "text", null);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
  }

  private List<Field> getFields(String family, String rowId, String recordId, Field... fields) {
    List<Field> fieldLst = new ArrayList<Field>();
    fieldLst.add(new StringField("family", family, Store.YES));
    fieldLst.add(new StringField("rowid", rowId, Store.YES));
    fieldLst.add(new StringField("recordid", recordId, Store.YES));
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
    assertEquals(expected.name(), actual.name());
    assertEquals(expected.stringValue(), actual.stringValue());
    assertEquals(expected.fieldType().toString(), actual.fieldType().toString());
  }

  private BaseFieldManager newBaseFieldManager() {
    return new BaseFieldManager() {
      @Override
      protected void tryToStore(String fieldName, boolean fieldLessIndexing, String fieldType, Map<String, String> props) {

      }
    };
  }

}
