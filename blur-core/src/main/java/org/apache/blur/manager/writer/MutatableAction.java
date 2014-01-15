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
package org.apache.blur.manager.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.manager.IndexManager;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurThriftRecord;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.ResetableDocumentStoredFieldVisitor;
import org.apache.blur.utils.RowDocumentUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

public class MutatableAction {

  static abstract class InternalAction {
    abstract void performAction(IndexReader reader, IndexWriter writer) throws IOException;
  }

  private final List<InternalAction> _actions = new ArrayList<InternalAction>();
  private final FieldManager _fieldManager;
  private final String _shard;
  private final String _table;
  private final Term _primeDocTerm;
  private final int _maxHeap = Integer.MAX_VALUE;

  public MutatableAction(ShardContext context) {
    TableContext tableContext = context.getTableContext();
    _shard = context.getShard();
    _table = tableContext.getTable();
    _fieldManager = tableContext.getFieldManager();
    _primeDocTerm = tableContext.getDefaultPrimeDocTerm();
  }

  public void deleteRow(final String rowId) {
    _actions.add(new InternalAction() {
      @Override
      void performAction(IndexReader reader, IndexWriter writer) throws IOException {
        writer.deleteDocuments(createRowId(rowId));
      }
    });
  }

  public void replaceRow(final Row row) {
    _actions.add(new InternalAction() {
      @Override
      void performAction(IndexReader reader, IndexWriter writer) throws IOException {
        List<List<Field>> docs = RowDocumentUtil.getDocs(row, _fieldManager);
        Term rowId = createRowId(row.getId());
        writer.updateDocuments(rowId, docs);
      }
    });
  }

  public void deleteRecord(final String rowId, final String recordId) {
    _actions.add(new InternalAction() {
      @Override
      void performAction(IndexReader reader, IndexWriter writer) throws IOException {
        Term rowIdTerm = createRowId(rowId);
        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(rowIdTerm), Occur.MUST);
        query.add(new TermQuery(BlurUtil.PRIME_DOC_TERM), Occur.MUST);

        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(query, 1);
        if (topDocs.totalHits == 0) {
          // do nothing
        } else if (topDocs.totalHits == 1) {
          Selector selector = new Selector();
          selector.setStartRecord(0);
          selector.setMaxRecordsToFetch(Integer.MAX_VALUE);
          selector.setLocationId(_shard + "/" + topDocs.scoreDocs[0].doc);
          ResetableDocumentStoredFieldVisitor fieldVisitor = IndexManager.getFieldSelector(selector);
          AtomicBoolean moreDocsToFetch = new AtomicBoolean(false);
          AtomicInteger totalRecords = new AtomicInteger();
          List<Document> docs = new ArrayList<Document>(BlurUtil.fetchDocuments(reader, fieldVisitor, selector,
              _maxHeap, _table + "/" + _shard, _primeDocTerm, null, moreDocsToFetch, totalRecords, null));
          if (moreDocsToFetch.get()) {
            throw new IOException("Row too large to update.");
          }
          boolean found = false;
          for (int i = 0; i < docs.size(); i++) {
            Document document = docs.get(i);
            if (document.get(BlurConstants.RECORD_ID).equals(recordId)) {
              docs.remove(i);
              found = true;
              break;
            }
          }
          if (found) {
            if (docs.size() == 0) {
              writer.deleteDocuments(rowIdTerm);
            } else {
              Row row = new Row(rowId, toRecords(docs));
              List<List<Field>> docsToUpdate = RowDocumentUtil.getDocs(row, _fieldManager);
              writer.updateDocuments(rowIdTerm, docsToUpdate);
            }
          }
        } else {
          throw new IOException("RowId [" + rowId + "] found more than one row primedoc.");
        }
      }
    });
  }

  public void replaceRecord(final String rowId, final Record record) {
    _actions.add(new InternalAction() {
      @Override
      void performAction(IndexReader reader, IndexWriter writer) throws IOException {
        Term rowIdTerm = createRowId(rowId);
        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(rowIdTerm), Occur.MUST);
        query.add(new TermQuery(BlurUtil.PRIME_DOC_TERM), Occur.MUST);

        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(query, 1);
        if (topDocs.totalHits == 0) {
          // just add
          List<Field> doc = RowDocumentUtil.getDoc(_fieldManager, rowId, record);
          doc.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
          writer.addDocument(doc);
        } else if (topDocs.totalHits == 1) {
          Selector selector = new Selector();
          selector.setStartRecord(0);
          selector.setMaxRecordsToFetch(Integer.MAX_VALUE);
          selector.setLocationId(_shard + "/" + topDocs.scoreDocs[0].doc);
          ResetableDocumentStoredFieldVisitor fieldVisitor = IndexManager.getFieldSelector(selector);
          AtomicBoolean moreDocsToFetch = new AtomicBoolean(false);
          AtomicInteger totalRecords = new AtomicInteger();
          List<Document> docs = new ArrayList<Document>(BlurUtil.fetchDocuments(reader, fieldVisitor, selector,
              _maxHeap, _table + "/" + _shard, _primeDocTerm, null, moreDocsToFetch, totalRecords, null));
          if (moreDocsToFetch.get()) {
            throw new IOException("Row too large to update.");
          }
          List<Field> doc = RowDocumentUtil.getDoc(_fieldManager, rowId, record);

          for (int i = 0; i < docs.size(); i++) {
            Document document = docs.get(i);
            if (document.get(BlurConstants.RECORD_ID).equals(record.getRecordId())) {
              docs.remove(i);
              break;
            }
          }
          docs.add(toDocument(doc));
          Row row = new Row(rowId, toRecords(docs));
          List<List<Field>> docsToUpdate = RowDocumentUtil.getDocs(row, _fieldManager);
          writer.updateDocuments(rowIdTerm, docsToUpdate);
        } else {
          throw new IOException("RowId [" + rowId + "] found more than one row primedoc.");
        }
      }
    });
  }

  private List<Record> toRecords(List<Document> docs) {
    List<Record> records = new ArrayList<Record>();
    for (Document document : docs) {
      BlurThriftRecord existingRecord = new BlurThriftRecord();
      RowDocumentUtil.readRecord(document, existingRecord);
      records.add(existingRecord);
    }
    return records;
  }

  public void appendColumns(final String rowId, final Record record) {
    _actions.add(new InternalAction() {
      @Override
      void performAction(IndexReader reader, IndexWriter writer) throws IOException {
        Term rowIdTerm = createRowId(rowId);
        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(rowIdTerm), Occur.MUST);
        query.add(new TermQuery(BlurUtil.PRIME_DOC_TERM), Occur.MUST);

        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(query, 1);
        if (topDocs.totalHits == 0) {
          // just add
          List<Field> doc = RowDocumentUtil.getDoc(_fieldManager, rowId, record);
          doc.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
          writer.addDocument(doc);
        } else if (topDocs.totalHits == 1) {
          Selector selector = new Selector();
          selector.setStartRecord(0);
          selector.setMaxRecordsToFetch(Integer.MAX_VALUE);
          selector.setLocationId(_shard + "/" + topDocs.scoreDocs[0].doc);
          ResetableDocumentStoredFieldVisitor fieldVisitor = IndexManager.getFieldSelector(selector);
          AtomicBoolean moreDocsToFetch = new AtomicBoolean(false);
          AtomicInteger totalRecords = new AtomicInteger();
          List<Document> docs = new ArrayList<Document>(BlurUtil.fetchDocuments(reader, fieldVisitor, selector,
              _maxHeap, _table + "/" + _shard, _primeDocTerm, null, moreDocsToFetch, totalRecords, null));
          if (moreDocsToFetch.get()) {
            throw new IOException("Row too large to update.");
          }
          BlurThriftRecord existingRecord = new BlurThriftRecord();
          for (int i = 0; i < docs.size(); i++) {
            Document document = docs.get(i);
            if (document.get(BlurConstants.RECORD_ID).equals(record.getRecordId())) {
              Document doc = docs.remove(i);
              RowDocumentUtil.readRecord(doc, existingRecord);
              break;
            }
          }

          String recordId = existingRecord.getRecordId();
          if (recordId == null) {
            existingRecord.setRecordId(record.getRecordId());
          } else if (!recordId.equals(record.getRecordId())) {
            throw new IOException("Record ids do not match.");
          }

          String family = existingRecord.getFamily();
          if (family == null) {
            existingRecord.setFamily(record.getFamily());
          } else if (!family.equals(record.getFamily())) {
            throw new IOException("Record family do not match.");
          }

          for (Column column : record.getColumns()) {
            existingRecord.addToColumns(column);
          }

          List<Field> doc = RowDocumentUtil.getDoc(_fieldManager, rowId, existingRecord);
          docs.add(toDocument(doc));

          Row row = new Row(rowId, toRecords(docs));
          List<List<Field>> docsToUpdate = RowDocumentUtil.getDocs(row, _fieldManager);

          writer.updateDocuments(rowIdTerm, docsToUpdate);
        } else {
          throw new IOException("RowId [" + rowId + "] found more than one row primedoc.");
        }
      }
    });
  }

  public void replaceColumns(final String rowId, final Record record) {
    _actions.add(new InternalAction() {
      @Override
      void performAction(IndexReader reader, IndexWriter writer) throws IOException {
        Term rowIdTerm = createRowId(rowId);
        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(rowIdTerm), Occur.MUST);
        query.add(new TermQuery(BlurUtil.PRIME_DOC_TERM), Occur.MUST);

        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(query, 1);
        if (topDocs.totalHits == 0) {
          // just add
          List<Field> doc = RowDocumentUtil.getDoc(_fieldManager, rowId, record);
          doc.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
          writer.addDocument(doc);
        } else if (topDocs.totalHits == 1) {
          Selector selector = new Selector();
          selector.setStartRecord(0);
          selector.setMaxRecordsToFetch(Integer.MAX_VALUE);
          selector.setLocationId(_shard + "/" + topDocs.scoreDocs[0].doc);
          ResetableDocumentStoredFieldVisitor fieldVisitor = IndexManager.getFieldSelector(selector);
          AtomicBoolean moreDocsToFetch = new AtomicBoolean(false);
          AtomicInteger totalRecords = new AtomicInteger();
          List<Document> docs = new ArrayList<Document>(BlurUtil.fetchDocuments(reader, fieldVisitor, selector,
              _maxHeap, _table + "/" + _shard, _primeDocTerm, null, moreDocsToFetch, totalRecords, null));
          if (moreDocsToFetch.get()) {
            throw new IOException("Row too large to update.");
          }
          BlurThriftRecord existingRecord = new BlurThriftRecord();
          for (int i = 0; i < docs.size(); i++) {
            Document document = docs.get(i);
            if (document.get(BlurConstants.RECORD_ID).equals(record.getRecordId())) {
              Document doc = docs.remove(i);
              RowDocumentUtil.readRecord(doc, existingRecord);
              break;
            }
          }

          Map<String, List<Column>> map = new HashMap<String, List<Column>>();
          for (Column column : record.getColumns()) {
            String name = column.getName();
            List<Column> list = map.get(name);
            if (list == null) {
              list = new ArrayList<Column>();
              map.put(name, list);
            }
            list.add(column);
          }

          Record newRecord = new Record(record.getRecordId(), record.getFamily(), null);
          Set<String> processedColumns = new HashSet<String>();
          List<Column> columns = existingRecord.getColumns();
          if (columns != null) {
            for (Column column : columns) {
              String name = column.getName();
              if (processedColumns.contains(name)) {
                continue;
              }
              List<Column> newColumns = map.get(name);
              if (newColumns != null) {
                for (Column c : newColumns) {
                  newRecord.addToColumns(c);
                }
                processedColumns.add(name);
              } else {
                newRecord.addToColumns(column);
              }
            }
          }

          for (Entry<String, List<Column>> e : map.entrySet()) {
            String name = e.getKey();
            if (processedColumns.contains(name)) {
              continue;
            }
            List<Column> newColumns = e.getValue();
            for (Column c : newColumns) {
              newRecord.addToColumns(c);
            }
            processedColumns.add(name);
          }

          List<Field> doc = RowDocumentUtil.getDoc(_fieldManager, rowId, newRecord);
          docs.add(toDocument(doc));

          Row row = new Row(rowId, toRecords(docs));
          List<List<Field>> docsToUpdate = RowDocumentUtil.getDocs(row, _fieldManager);
          writer.updateDocuments(rowIdTerm, docsToUpdate);
        } else {
          throw new IOException("RowId [" + rowId + "] found more than one row primedoc.");
        }
      }
    });
  }

  private Document toDocument(List<Field> doc) {
    Document document = new Document();
    for (Field f : doc) {
      document.add(f);
    }
    return document;
  }

  void performMutate(IndexReader reader, IndexWriter writer) throws IOException {
    try {
      for (InternalAction internalAction : _actions) {
        internalAction.performAction(reader, writer);
      }
    } finally {
      _actions.clear();
    }
  }

  public static Term createRowId(String id) {
    return new Term(BlurConstants.ROW_ID, id);
  }

  public static Term createRecordId(String id) {
    return new Term(BlurConstants.RECORD_ID, id);
  }

}
