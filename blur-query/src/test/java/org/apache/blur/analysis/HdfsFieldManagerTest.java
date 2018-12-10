package org.apache.blur.analysis;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.junit.Test;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class HdfsFieldManagerTest extends BaseFieldManagerTest {
  private static final String DFS_FIELD_MANAGER_PATH = "./target/tmp/HdfsFieldManagerTest/meta";

  @Override
  protected BaseFieldManager newFieldManager(boolean create) throws IOException {
    Configuration config = new Configuration();
    Path path = new Path(DFS_FIELD_MANAGER_PATH);
    FileSystem fileSystem = path.getFileSystem(config);
    if (create) {
      fileSystem.delete(path, true);
    }
    return new HdfsFieldManager(_fieldLessField, new KeywordAnalyzer(), path, config);
  }

  @Test
  public void fieldManagerShouldIgnoreUnknownFiles() throws IOException {
    BaseFieldManager fieldManager = newFieldManager(true);
    fieldManager.addColumnDefinition("unknowntest", "col1", null, true, "text", false, false, null);
    FieldTypeDefinition fieldTypeDefinition1 = fieldManager.getFieldTypeDefinition("unknowntest.col1");

    assertNotNull(fieldTypeDefinition1);
    Path path = new Path(DFS_FIELD_MANAGER_PATH, "mydoc.txt");
    FSDataOutputStream outputStream = path.getFileSystem(new Configuration()).create(path, false);
    IOUtils.write("Some text..", outputStream);
    outputStream.close();

    BaseFieldManager fieldManager2 = newFieldManager(false);
    fieldManager2.loadFromStorage();
    FieldTypeDefinition fieldTypeDefinition2 = fieldManager2.getFieldTypeDefinition("unknowntest.col1");
    assertNotNull(fieldTypeDefinition2);
  }

  @Test
  public void columnsWithNamesSimilarToOurSuffixShouldntCauseProblems() throws IOException {
    BaseFieldManager fieldManager = newFieldManager(true);
    fieldManager.addColumnDefinition("similartest", "typeFoo", null, true, "text", false, false, null);
    FieldTypeDefinition fieldTypeDefinition1 = fieldManager.getFieldTypeDefinition("similartest.typeFoo");

    assertNotNull(fieldTypeDefinition1);
    assertEquals("typeFoo", fieldTypeDefinition1.getColumnName());

    assertTrue(fieldManager.getFieldNames().contains("similartest.typeFoo"));
    BaseFieldManager fieldManager2 = newFieldManager(false);
    fieldManager2.loadFromStorage();

    assertTrue("Should have the real field.", fieldManager2.getFieldNames().contains("similartest.typeFoo"));
    FieldTypeDefinition fieldTypeDefinition2 = fieldManager2.getFieldTypeDefinition("similartest.typeFoo");
    assertNotNull(fieldTypeDefinition2);
    assertEquals("typeFoo", fieldTypeDefinition2.getColumnName());
    assertEquals("similartest", fieldTypeDefinition2.getFamily());
    assertEquals("text", fieldTypeDefinition2.getFieldType());

    fieldTypeDefinition2 = fieldManager2.getFieldTypeDefinition("similartestFoo");

    assertNull("This one shouldn't exist.", fieldTypeDefinition2);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStoreMetaData() throws IOException {
    BaseFieldManager memoryFieldManager = newFieldManager(true);
    memoryFieldManager.addColumnDefinition("fam1", "col1", null, true, "text", false, false, null);
    memoryFieldManager.addColumnDefinition("fam2", "col2", null, false, "string", true, false, null);
    memoryFieldManager.addColumnDefinition("fam2", "col2", "a", false, "text", false, false, null);
    memoryFieldManager.addColumnDefinition("fam2", "col2", "b", false, "text", false, false, newMap(e("a", "b")));
    memoryFieldManager.addColumnDefinition("fam2", "col3", null, false, "int", false, false, null);

    BaseFieldManager memoryFieldManager2 = newFieldManager(false);
    FieldTypeDefinition fieldTypeDefinition1 = memoryFieldManager2.getFieldTypeDefinition("fam1.col1");
    assertNotNull(fieldTypeDefinition1);
    assertTrue(fieldTypeDefinition1.isFieldLessIndexed());
    assertFalse(fieldTypeDefinition1.isNumeric());

    FieldTypeDefinition fieldTypeDefinition2 = memoryFieldManager2.getFieldTypeDefinition("fam2.col2");
    assertNotNull(fieldTypeDefinition2);
    assertFalse(fieldTypeDefinition2.isFieldLessIndexed());
    assertFalse(fieldTypeDefinition2.isNumeric());

    FieldTypeDefinition fieldTypeDefinition3 = memoryFieldManager2.getFieldTypeDefinition("fam2.col2.a");
    assertNotNull(fieldTypeDefinition3);
    assertFalse(fieldTypeDefinition3.isFieldLessIndexed());
    assertFalse(fieldTypeDefinition3.isNumeric());

    FieldTypeDefinition fieldTypeDefinition4 = memoryFieldManager2.getFieldTypeDefinition("fam2.col2.b");
    assertNotNull(fieldTypeDefinition4);
    assertFalse(fieldTypeDefinition4.isFieldLessIndexed());
    assertFalse(fieldTypeDefinition4.isNumeric());

    FieldTypeDefinition fieldTypeDefinition5 = memoryFieldManager2.getFieldTypeDefinition("fam2.col3");
    assertNotNull(fieldTypeDefinition5);
    assertFalse(fieldTypeDefinition5.isFieldLessIndexed());
    assertTrue(fieldTypeDefinition5.isNumeric());

    assertNull(memoryFieldManager2.getFieldTypeDefinition("fam2.col2.c"));
  }

  private static <K, V> Map<K, V> newMap(Entry<K, V>... es) {
    Map<K, V> map = new HashMap<K, V>();
    for (Entry<K, V> e : es) {
      map.put(e.getKey(), e.getValue());
    }
    return map;
  }

  private static <K, V> Entry<K, V> e(final K key, final V value) {
    return new Entry<K, V>() {

      @Override
      public V setValue(V value) {
        return null;
      }

      @Override
      public V getValue() {
        return value;
      }

      @Override
      public K getKey() {
        return key;
      }
    };
  }
}
