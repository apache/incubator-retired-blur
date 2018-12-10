package org.apache.blur.utils;

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
import static org.apache.blur.utils.BlurConstants.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;

public class RowDocumentUtil {

  public static FieldType ID_TYPE;
  static {
    ID_TYPE = new FieldType();
    ID_TYPE.setIndexed(true);
    ID_TYPE.setTokenized(false);
    ID_TYPE.setOmitNorms(true);
    ID_TYPE.setStored(true);
    ID_TYPE.freeze();
  }

  public static FetchRecordResult getRecord(Document document) {
    FetchRecordResult result = new FetchRecordResult();
    BlurThriftRecord record = new BlurThriftRecord();
    String rowId = readRecord(document, record);
    result.setRecord(record);
    result.setRowid(rowId);
    return result;
  }

  public static Row getRow(Iterable<Document> docs) {
    Row row = new Row();
    boolean empty = true;
    if (docs == null) {
      return null;
    }
    for (Document document : docs) {
      empty = false;
      BlurThriftRecord record = new BlurThriftRecord();
      String rowId = readRecord(document, record);
      if (record.getColumns() != null) {
        row.addToRecords(record);
      }
      if (row.id == null) {
        row.setId(rowId);
      }
    }
    if (empty) {
      return null;
    }
    if (row.records == null) {
      row.records = new ArrayList<Record>();
    }
    return row;
  }

  public static String readRecord(Document document, ReaderBlurRecord reader) {
    String rowId = null;
    for (IndexableField field : document.getFields()) {
      if (field.name().equals(ROW_ID)) {
        rowId = field.stringValue();
      } else if (field.name().equals(RECORD_ID)) {
        reader.setRecordIdStr(field.stringValue());
      } else if (field.name().equals(FAMILY)) {
        reader.setFamilyStr(field.stringValue());
      } else {
        String name = field.name();
        int index = name.indexOf(SEP);
        if (index < 0) {
          continue;
        }
        name = name.substring(index + 1);
        reader.addColumn(name, field.stringValue());
      }
    }
    return rowId;
  }

  public static List<List<Field>> getDocs(Row row, FieldManager fieldManager) throws IOException {
    List<Record> records = row.records;
    if (records == null) {
      return null;
    }
    int size = records.size();
    if (size == 0) {
      return null;
    }
    final String rowId = row.id;
    List<List<Field>> docs = new ArrayList<List<Field>>(size);
    for (int i = 0; i < size; i++) {
      Record record = records.get(i);
      List<Field> fields = getDoc(fieldManager, rowId, record);
      docs.add(fields);
    }
    List<Field> doc = docs.get(0);
    doc.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
    return docs;
  }

  public static List<Field> getDoc(FieldManager fieldManager, final String rowId, Record record) throws IOException {
    ShardUtil.validateRowIdAndRecord(rowId, record);
    List<Field> fields = fieldManager.getFields(rowId, record);
    return fields;
  }

  public static Term createRowId(String id) {
    return new Term(BlurConstants.ROW_ID, id);
  }
}
