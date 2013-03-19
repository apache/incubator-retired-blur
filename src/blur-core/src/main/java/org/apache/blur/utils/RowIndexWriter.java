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
import static org.apache.blur.utils.BlurConstants.SUPER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;


public class RowIndexWriter {

  private BlurAnalyzer _analyzer;
  private IndexWriter _indexWriter;
  private boolean primeDocSet;
  private StringBuilder builder = new StringBuilder();

  public RowIndexWriter(IndexWriter indexWriter, BlurAnalyzer analyzer) {
    _indexWriter = indexWriter;
    _analyzer = analyzer;
  }

  public void add(boolean wal, Row row) throws IOException {
    if (row == null || row.id == null) {
      throw new NullPointerException();
    }
    append(wal, row, false);
  }

  public void replace(boolean wal, Row row) throws IOException {
    if (row == null || row.id == null) {
      throw new NullPointerException();
    }
    append(wal, row, true);
  }

  private void append(boolean wal, Row row, boolean replace) throws IOException {
    primeDocSet = false;
    List<Document> documents = new ArrayList<Document>();
    for (Record record : row.records) {
      convert(row.id, record, documents);
    }
    if (replace) {
      _indexWriter.updateDocuments(new Term(ROW_ID, row.id), documents, _analyzer);
    } else {
      _indexWriter.addDocuments(documents, _analyzer);
    }
  }

  private void convert(String rowId, Record record, List<Document> documents) throws IOException {
    if (record == null) {
      return;
    }
    String recordId = record.recordId;
    if (recordId == null) {
      throw new NullPointerException("Record id is null.");
    }
    String family = record.getFamily();
    if (family == null) {
      throw new NullPointerException("Family is null.");
    }
    Document document = new Document();
    document.add(new Field(ROW_ID, rowId, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    document.add(new Field(RECORD_ID, recordId, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    if (addColumns(document, _analyzer, builder, family, record.columns)) {
      if (!primeDocSet) {
        document.add(new Field(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        primeDocSet = true;
      }
      documents.add(document);
    }
  }

  public static boolean addColumns(Document document, BlurAnalyzer analyzer, StringBuilder builder, String columnFamily, Iterable<Column> set) {
    if (set == null) {
      return false;
    }
    builder.setLength(0);
    OUTER: for (Column column : set) {
      String name = column.getName();
      String value = column.value;
      if (value == null || name == null) {
        continue OUTER;
      }
      String fieldName = getFieldName(columnFamily, name);
      Store store = analyzer.getStore(fieldName);
      Index index = analyzer.getIndex(fieldName);
      boolean fullText = analyzer.isFullTextField(fieldName);
      Set<String> subFieldNames = analyzer.getSubIndexNames(fieldName);
      document.add(new Field(fieldName, value, store, index));
      if (fullText) {
        builder.append(value).append(' ');
      }
      if (subFieldNames != null) {
        for (String subFieldName : subFieldNames) {
          document.add(new Field(subFieldName, value, Store.NO, index));
        }
      }
    }
    if (builder.length() != 0) {
      String superValue = builder.toString();
      document.add(new Field(SUPER, superValue, Store.NO, Index.ANALYZED_NO_NORMS));
    }
    return true;
  }

  public static String getFieldName(String columnFamily, String name) {
    return columnFamily + SEP + name;
  }

}
