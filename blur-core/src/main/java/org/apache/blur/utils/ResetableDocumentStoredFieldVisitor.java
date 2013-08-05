package org.apache.blur.utils;

/*
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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFieldVisitor;

/**
 * A {@link StoredFieldVisitor} that creates a {@link Document} containing all
 * stored fields, or only specific requested fields provided to
 * {@link #DocumentStoredFieldVisitor(Set)}.
 * <p>
 * This is used by {@link IndexReader#document(int)} to load a document.
 * 
 * @lucene.experimental
 */

public class ResetableDocumentStoredFieldVisitor extends StoredFieldVisitor {

  private static final int _emptyDocumentSize;
  private static final int _integerFieldSize;
  private static final int _longFieldSize;
  private static final int _doubleFieldSize;
  private static final int _floatFieldSize;
  private static final int _emptyString;
  private static final int _emptyByteArrayFieldSize;

  static {
    _emptyDocumentSize = (int) RamUsageEstimator.sizeOf(new Document());
    _integerFieldSize = (int) RamUsageEstimator.sizeOf(new StoredField("", 0));
    _longFieldSize = (int) RamUsageEstimator.sizeOf(new StoredField("", 0l));
    _doubleFieldSize = (int) RamUsageEstimator.sizeOf(new StoredField("", 0.0));
    _floatFieldSize = (int) RamUsageEstimator.sizeOf(new StoredField("", 0.0f));
    _emptyByteArrayFieldSize = (int) RamUsageEstimator.sizeOf(new StoredField("", new byte[] {}));
    _emptyString = (int) RamUsageEstimator.sizeOf("");
  }

  private Document doc = new Document();
  private final Set<String> fieldsToAdd;
  private int size = _emptyDocumentSize;

  /** Load only fields named in the provided <code>Set&lt;String&gt;</code>. */
  public ResetableDocumentStoredFieldVisitor(Set<String> fieldsToAdd) {
    this.fieldsToAdd = fieldsToAdd;
  }

  /** Load only fields named in the provided <code>Set&lt;String&gt;</code>. */
  public ResetableDocumentStoredFieldVisitor(String... fields) {
    fieldsToAdd = new HashSet<String>(fields.length);
    for (String field : fields) {
      fieldsToAdd.add(field);
    }
  }

  /** Load all stored fields. */
  public ResetableDocumentStoredFieldVisitor() {
    this.fieldsToAdd = null;
  }

  @Override
  public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
    doc.add(new StoredField(fieldInfo.name, value));
    addSize(fieldInfo, _emptyByteArrayFieldSize);
    size += value.length;
  }

  @Override
  public void stringField(FieldInfo fieldInfo, String value) throws IOException {
    final FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(fieldInfo.hasVectors());
    ft.setIndexed(fieldInfo.isIndexed());
    ft.setOmitNorms(fieldInfo.omitsNorms());
    ft.setIndexOptions(fieldInfo.getIndexOptions());
    doc.add(new Field(fieldInfo.name, value, ft));
    size += _emptyString * 2;
    size += fieldInfo.name.length() * 2;
    size += value.length() * 2;
  }

  @Override
  public void intField(FieldInfo fieldInfo, int value) {
    doc.add(new StoredField(fieldInfo.name, value));
    addSize(fieldInfo, _integerFieldSize);
  }

  @Override
  public void longField(FieldInfo fieldInfo, long value) {
    doc.add(new StoredField(fieldInfo.name, value));
    addSize(fieldInfo, _longFieldSize);
  }

  @Override
  public void floatField(FieldInfo fieldInfo, float value) {
    doc.add(new StoredField(fieldInfo.name, value));
    addSize(fieldInfo, _floatFieldSize);
  }

  @Override
  public void doubleField(FieldInfo fieldInfo, double value) {
    doc.add(new StoredField(fieldInfo.name, value));
    addSize(fieldInfo, _doubleFieldSize);
  }

  private void addSize(FieldInfo fieldInfo, int fieldSize) {
    size += fieldSize;
    size += _emptyString;
    size += fieldInfo.name.length() * 2;
  }

  @Override
  public Status needsField(FieldInfo fieldInfo) throws IOException {
    return fieldsToAdd == null || fieldsToAdd.contains(fieldInfo.name) ? Status.YES : Status.NO;
  }

  /**
   * Retrieve the visited document.
   * 
   * @return Document populated with stored fields. Note that only the stored
   *         information in the field instances is valid, data such as boosts,
   *         indexing options, term vector options, etc is not set.
   */
  public Document getDocument() {
    return doc;
  }

  public int getSize() {
    return size;
  }

  public void reset() {
    doc = new Document();
    size = _emptyDocumentSize;
  }
}
