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
import static org.apache.blur.utils.BlurConstants.RECORD_ID;
import static org.apache.blur.utils.BlurConstants.ROW_ID;
import static org.apache.blur.utils.BlurConstants.SEP;

import java.util.ArrayList;

import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;


public class RowDocumentUtil {

  public static FetchRecordResult getColumns(Document document) {
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
      row.recordCount++;
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
    String family = null;
    for (Fieldable field : document.getFields()) {
      if (field.name().equals(ROW_ID)) {
        rowId = field.stringValue();
      } else if (field.name().equals(RECORD_ID)) {
        reader.setRecordIdStr(field.stringValue());
      } else {
        String name = field.name();
        int index = name.indexOf(SEP);
        if (index < 0) {
          continue;
        } else if (family == null) {
          family = name.substring(0, index);
        }
        name = name.substring(index + 1);
        reader.addColumn(name, field.stringValue());
      }
    }
    reader.setFamilyStr(family);
    reader.setRowIdStr(rowId);
    return rowId;
  }
}
