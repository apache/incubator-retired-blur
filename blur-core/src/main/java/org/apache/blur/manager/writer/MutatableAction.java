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

import static org.apache.blur.metrics.MetricsConstants.BLUR;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.MutationHelper;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.RowDocumentUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class MutatableAction extends IndexAction {

  private static final Meter _writeRecordsMeter;
  private static final Meter _writeRowMeter;

  static {
    MetricName metricName1 = new MetricName(ORG_APACHE_BLUR, BLUR, "Write Records/s");
    MetricName metricName2 = new MetricName(ORG_APACHE_BLUR, BLUR, "Write Row/s");
    _writeRecordsMeter = Metrics.newMeter(metricName1, "Records/s", TimeUnit.SECONDS);
    _writeRowMeter = Metrics.newMeter(metricName2, "Row/s", TimeUnit.SECONDS);
  }

  static abstract class BaseRecordMutatorIterator implements Iterable<Record> {

    private final Iterable<Record> _iterable;
    private final Map<String, Record> _records;

    public BaseRecordMutatorIterator(Iterable<Record> iterable, List<Record> records) {
      _iterable = iterable;
      _records = new TreeMap<String, Record>();
      for (Record r : records) {
        _records.put(r.getRecordId(), r);
      }
    }

    protected abstract Record handleRecordMutate(Record existingRecord, Record newRecord);

    @Override
    public Iterator<Record> iterator() {
      final Iterator<Record> iterator = _iterable.iterator();
      return new Iterator<Record>() {

        private SortedSet<String> _needToBeApplied = new TreeSet<String>(_records.keySet());
        private boolean _append = false;

        @Override
        public boolean hasNext() {
          boolean hasNext = iterator.hasNext();
          if (hasNext) {
            return true;
          }
          if (areAllApplied()) {
            return false; // Already applied changes, finished.
          }
          _append = true;
          return true; // Still need to add new records.
        }

        private boolean areAllApplied() {
          return _needToBeApplied.size() == 0;
        }

        @Override
        public Record next() {
          if (_append) {
            String first = _needToBeApplied.first();
            _needToBeApplied.remove(first);
            return _records.get(first);
          }
          Record record = iterator.next();
          String recordId = record.getRecordId();
          Record newRecord = _records.get(recordId);
          if (newRecord != null) {
            record = handleRecordMutate(record, newRecord);
            _needToBeApplied.remove(recordId);
          }
          return record;
        }

        @Override
        public void remove() {
          throw new RuntimeException("Not Supported.");
        }

      };
    }
  }

  static class UpdateRow extends InternalAction {

    static abstract class UpdateRowAction {
      abstract IterableRow performAction(IterableRow row);
    }

    private final List<UpdateRowAction> _actions = new ArrayList<UpdateRowAction>();

    private UpdateRowAction _deleteRecordsAction;
    private final Set<String> _deleteRecordsActionRecordsIdToDelete = new HashSet<String>();
    private UpdateRowAction _appendColumnsAction;
    private List<Record> _appendColumnsActionRecords = new ArrayList<Record>();
    private UpdateRowAction _replaceColumnsAction;
    private List<Record> _replaceColumnsActionRecords = new ArrayList<Record>();
    private UpdateRowAction _replaceRecordAction;
    private List<Record> _replaceRecordActionRecords = new ArrayList<Record>();

    private final String _rowId;
    private final TableContext _tableContext;
    private final FieldManager _fieldManager;

    UpdateRow(String rowId, TableContext tableContext) {
      _rowId = rowId;
      _tableContext = tableContext;
      _fieldManager = _tableContext.getFieldManager();
    }

    void deleteRecord(final String recordId) {
      if (_deleteRecordsAction == null) {
        _deleteRecordsAction = new UpdateRowAction() {
          @Override
          IterableRow performAction(IterableRow row) {
            if (row == null) {
              return null;
            } else {
              return new IterableRow(row.getRowId(), new DeleteRecordIterator(row,
                  _deleteRecordsActionRecordsIdToDelete));
            }
          }
        };
        _actions.add(_deleteRecordsAction);
      }
      _deleteRecordsActionRecordsIdToDelete.add(recordId);
    }

    static class DeleteRecordIterator implements Iterable<Record> {

      private final Set<String> _recordsIdToDelete;
      private final Iterable<Record> _iterable;

      public DeleteRecordIterator(Iterable<Record> iterable, Set<String> recordsIdToDelete) {
        _recordsIdToDelete = recordsIdToDelete;
        _iterable = iterable;
      }

      @Override
      public Iterator<Record> iterator() {
        final GenericPeekableIterator<Record> iterator = GenericPeekableIterator.wrap(_iterable.iterator());
        return new Iterator<Record>() {

          @Override
          public boolean hasNext() {
            Record record = iterator.peek();
            if (record == null) {
              return false;
            }
            if (_recordsIdToDelete.contains(record.getRecordId())) {
              iterator.next();// Eat the delete
              return hasNext();// Move to the next record
            }
            return iterator.hasNext();
          }

          @Override
          public Record next() {
            return iterator.next();
          }

          @Override
          public void remove() {
            throw new RuntimeException("Not Supported.");
          }
        };
      }

    }

    void appendColumns(final Record record) {
      if (_appendColumnsAction == null) {
        _appendColumnsAction = new UpdateRowAction() {
          @Override
          IterableRow performAction(IterableRow row) {
            if (row == null) {
              return new IterableRow(_rowId, _appendColumnsActionRecords);
            } else {
              return new IterableRow(row.getRowId(), new AppendColumnsIterator(row, _appendColumnsActionRecords));
            }
          }
        };
        _actions.add(_appendColumnsAction);
      }
      _appendColumnsActionRecords.add(record);
    }

    static class AppendColumnsIterator extends BaseRecordMutatorIterator {

      public AppendColumnsIterator(Iterable<Record> iterable, List<Record> records) {
        super(iterable, records);
      }

      @Override
      protected Record handleRecordMutate(Record existingRecord, Record newRecord) {
        for (Column column : newRecord.getColumns()) {
          existingRecord.addToColumns(column);
        }
        return existingRecord;
      }

    }

    void replaceColumns(final Record record) {
      if (_replaceColumnsAction == null) {
        _replaceColumnsAction = new UpdateRowAction() {
          @Override
          IterableRow performAction(IterableRow row) {
            if (row == null) {
              return new IterableRow(_rowId, _replaceColumnsActionRecords);
            } else {
              return new IterableRow(row.getRowId(), new ReplaceColumnsIterator(row, _replaceColumnsActionRecords));
            }
          }
        };
        _actions.add(_replaceColumnsAction);
      }
      _replaceColumnsActionRecords.add(record);
    }

    static class ReplaceColumnsIterator extends BaseRecordMutatorIterator {

      public ReplaceColumnsIterator(Iterable<Record> iterable, List<Record> records) {
        super(iterable, records);
      }

      @Override
      protected Record handleRecordMutate(Record existingRecord, Record newRecord) {
        return replaceColumns(existingRecord, newRecord);
      }

    }

    protected static Record replaceColumns(Record existing, Record newRecord) {
      Map<String, List<Column>> existingColumns = getColumnMap(existing.getColumns());
      Map<String, List<Column>> newColumns = getColumnMap(newRecord.getColumns());
      existingColumns.putAll(newColumns);
      Record record = new Record();
      record.setFamily(existing.getFamily());
      record.setRecordId(existing.getRecordId());
      record.setColumns(toList(existingColumns.values()));
      return record;
    }

    private static List<Column> toList(Collection<List<Column>> values) {
      ArrayList<Column> list = new ArrayList<Column>();
      for (List<Column> v : values) {
        list.addAll(v);
      }
      return list;
    }

    private static Map<String, List<Column>> getColumnMap(List<Column> columns) {
      Map<String, List<Column>> columnMap = new TreeMap<String, List<Column>>();
      for (Column column : columns) {
        String name = column.getName();
        List<Column> list = columnMap.get(name);
        if (list == null) {
          list = new ArrayList<Column>();
          columnMap.put(name, list);
        }
        list.add(column);
      }
      return columnMap;
    }

    void replaceRecord(final Record record) {
      if (_replaceRecordAction == null) {
        _replaceRecordAction = new UpdateRowAction() {
          @Override
          IterableRow performAction(IterableRow row) {
            if (row == null) {
              // New Row
              return new IterableRow(_rowId, _replaceRecordActionRecords);
            } else {
              // Existing Row
              return new IterableRow(row.getRowId(), new ReplaceRecordIterator(row, _replaceRecordActionRecords));
            }
          }
        };
        _actions.add(_replaceRecordAction);
      }
      _replaceRecordActionRecords.add(record);
    }

    static class ReplaceRecordIterator extends BaseRecordMutatorIterator {

      public ReplaceRecordIterator(Iterable<Record> iterable, List<Record> records) {
        super(iterable, records);
      }

      @Override
      protected Record handleRecordMutate(Record existingRecord, Record newRecord) {
        return newRecord;
      }

    }

    @Override
    void performAction(IndexSearcherCloseable searcher, IndexWriter writer) throws IOException {
      IterableRow iterableRow = getIterableRow(_rowId, searcher);
      for (UpdateRowAction action : _actions) {
        iterableRow = action.performAction(iterableRow);
      }
      Term term = createRowId(_rowId);
      if (iterableRow != null) {
        RecordToDocumentIterable docsToUpdate = new RecordToDocumentIterable(iterableRow, _fieldManager);
        Iterator<Iterable<Field>> iterator = docsToUpdate.iterator();
        final GenericPeekableIterator<Iterable<Field>> gpi = GenericPeekableIterator.wrap(iterator);
        if (gpi.peek() != null) {
          writer.updateDocuments(term, wrapPrimeDoc(new Iterable<Iterable<Field>>() {
            @Override
            public Iterator<Iterable<Field>> iterator() {
              return gpi;
            }
          }));
        } else {
          writer.deleteDocuments(term);
        }
        _writeRecordsMeter.mark(docsToUpdate.count());
      }
      _writeRowMeter.mark();
    }

    private static class AtomicReaderTermsEnum {
      AtomicReader _atomicReader;
      TermsEnum _termsEnum;

      AtomicReaderTermsEnum(AtomicReader atomicReader, TermsEnum termsEnum) {
        _atomicReader = atomicReader;
        _termsEnum = termsEnum;
      }
    }

    private IterableRow getIterableRow(String rowId, IndexSearcherCloseable searcher) throws IOException {
      IndexReader indexReader = searcher.getIndexReader();
      BytesRef rowIdRef = new BytesRef(rowId);
      List<AtomicReaderTermsEnum> possibleRowIds = new ArrayList<AtomicReaderTermsEnum>();
      for (AtomicReaderContext atomicReaderContext : indexReader.leaves()) {
        AtomicReader atomicReader = atomicReaderContext.reader();
        Fields fields = atomicReader.fields();
        if (fields == null) {
          continue;
        }
        Terms terms = fields.terms(BlurConstants.ROW_ID);
        if (terms == null) {
          continue;
        }
        TermsEnum termsEnum = terms.iterator(null);
        if (!termsEnum.seekExact(rowIdRef, true)) {
          continue;
        }
        // need atomic read as well...
        possibleRowIds.add(new AtomicReaderTermsEnum(atomicReader, termsEnum));
      }
      if (possibleRowIds.isEmpty()) {
        return null;
      }
      return new IterableRow(rowId, getRecords(possibleRowIds));
    }

    private Iterable<Record> getRecords(final List<AtomicReaderTermsEnum> possibleRowIds) {
      return new Iterable<Record>() {
        @Override
        public Iterator<Record> iterator() {
          final List<DocsEnum> docsEnums = new ArrayList<DocsEnum>();
          for (AtomicReaderTermsEnum atomicReaderTermsEnum : possibleRowIds) {
            try {
              docsEnums.add(atomicReaderTermsEnum._termsEnum.docs(atomicReaderTermsEnum._atomicReader.getLiveDocs(),
                  null));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
          return new Iterator<Record>() {

            private int _index = 0;
            private boolean _nextCalled;
            private int _docId;

            @Override
            public boolean hasNext() {
              try {
                if (_nextCalled) {
                  if (_docId == DocIdSetIterator.NO_MORE_DOCS) {
                    return false;
                  }
                  return true;
                }
                while (true) {
                  if (_index >= docsEnums.size()) {
                    _nextCalled = true;
                    _docId = DocIdSetIterator.NO_MORE_DOCS;
                    return false;
                  }
                  DocsEnum docsEnum = docsEnums.get(_index);
                  int docId = docsEnum.nextDoc();
                  if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                    _nextCalled = true;
                    _docId = docId;
                    return true;
                  }
                  _index++;
                }
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public Record next() {
              _nextCalled = false;
              AtomicReaderTermsEnum atomicReaderTermsEnum = possibleRowIds.get(_index);
              try {
                Document document = atomicReaderTermsEnum._atomicReader.document(_docId);
                FetchRecordResult fetchRecordResult = RowDocumentUtil.getRecord(document);
                return fetchRecordResult.getRecord();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public void remove() {
              throw new RuntimeException("Not Supported.");
            }
          };
        }
      };
    }

    Iterable<Iterable<Field>> wrapPrimeDoc(final Iterable<Iterable<Field>> iterable) {
      return new Iterable<Iterable<Field>>() {

        @Override
        public Iterator<Iterable<Field>> iterator() {
          final Iterator<Iterable<Field>> iterator = iterable.iterator();
          return new Iterator<Iterable<Field>>() {

            private boolean _first = true;

            @Override
            public boolean hasNext() {
              return iterator.hasNext();
            }

            @Override
            public Iterable<Field> next() {
              Iterable<Field> fields = iterator.next();
              if (_first) {
                _first = false;
                return addPrimeDocField(fields);
              } else {
                return fields;
              }
            }

            private Iterable<Field> addPrimeDocField(Iterable<Field> fields) {
              return new IterablePlusOne<Field>(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE,
                  Store.NO), fields);
            }

            @Override
            public void remove() {
              throw new RuntimeException("Not Supported.");
            }

          };
        }
      };
    }

  }

  static abstract class InternalAction {
    abstract void performAction(IndexSearcherCloseable searcher, IndexWriter writer) throws IOException;
  }

  private final List<InternalAction> _actions = new ArrayList<InternalAction>();
  private final Map<String, UpdateRow> _rowUpdates = new HashMap<String, UpdateRow>();
  private final FieldManager _fieldManager;
  private final TableContext _tableContext;

  public MutatableAction(ShardContext context) {
    _tableContext = context.getTableContext();
    _fieldManager = _tableContext.getFieldManager();
  }

  public void deleteRow(final String rowId) {
    _actions.add(new InternalAction() {
      @Override
      void performAction(IndexSearcherCloseable searcher, IndexWriter writer) throws IOException {
        writer.deleteDocuments(createRowId(rowId));
        _writeRowMeter.mark();
      }
    });
  }

  public void replaceRow(final Row row) {
    _actions.add(new InternalAction() {
      @Override
      void performAction(IndexSearcherCloseable searcher, IndexWriter writer) throws IOException {
        List<List<Field>> docs = RowDocumentUtil.getDocs(row, _fieldManager);
        Term rowId = createRowId(row.getId());
        writer.updateDocuments(rowId, docs);
        _writeRecordsMeter.mark(docs.size());
        _writeRowMeter.mark();
      }
    });
  }

  public void deleteRecord(final String rowId, final String recordId) {
    UpdateRow updateRow = getUpdateRow(rowId);
    updateRow.deleteRecord(recordId);
  }

  public void replaceRecord(final String rowId, final Record record) {
    UpdateRow updateRow = getUpdateRow(rowId);
    updateRow.replaceRecord(record);
  }

  public void appendColumns(final String rowId, final Record record) {
    UpdateRow updateRow = getUpdateRow(rowId);
    updateRow.appendColumns(record);
  }

  public void replaceColumns(final String rowId, final Record record) {
    UpdateRow updateRow = getUpdateRow(rowId);
    updateRow.replaceColumns(record);
  }

  @Override
  public void performMutate(IndexSearcherCloseable searcher, IndexWriter writer) throws IOException {
    try {
      for (InternalAction internalAction : _actions) {
        internalAction.performAction(searcher, writer);
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

  private synchronized UpdateRow getUpdateRow(String rowId) {
    UpdateRow updateRow = _rowUpdates.get(rowId);
    if (updateRow == null) {
      updateRow = new UpdateRow(rowId, _tableContext);
      _rowUpdates.put(rowId, updateRow);
      _actions.add(updateRow);
    }
    return updateRow;
  }

  @Override
  public void doPreCommit(IndexSearcherCloseable indexSearcher, IndexWriter writer) {

  }

  @Override
  public void doPostCommit(IndexWriter writer) {

  }

  @Override
  public void doPreRollback(IndexWriter writer) {

  }

  @Override
  public void doPostRollback(IndexWriter writer) {

  }

  public void mutate(RowMutation mutation) {
    RowMutationType type = mutation.rowMutationType;
    switch (type) {
    case REPLACE_ROW:
      Row row = MutationHelper.getRowFromMutations(mutation.rowId, mutation.recordMutations);
      replaceRow(row);
      break;
    case UPDATE_ROW:
      doUpdateRowMutation(mutation, this);
      break;
    case DELETE_ROW:
      deleteRow(mutation.rowId);
      break;
    default:
      throw new RuntimeException("Not supported [" + type + "]");
    }
  }

  private void doUpdateRowMutation(RowMutation mutation, MutatableAction mutatableAction) {
    String rowId = mutation.getRowId();
    for (RecordMutation recordMutation : mutation.getRecordMutations()) {
      RecordMutationType type = recordMutation.recordMutationType;
      Record record = recordMutation.getRecord();
      switch (type) {
      case DELETE_ENTIRE_RECORD:
        mutatableAction.deleteRecord(rowId, record.getRecordId());
        break;
      case APPEND_COLUMN_VALUES:
        mutatableAction.appendColumns(rowId, record);
        break;
      case REPLACE_ENTIRE_RECORD:
        mutatableAction.replaceRecord(rowId, record);
        break;
      case REPLACE_COLUMNS:
        mutatableAction.replaceColumns(rowId, record);
        break;
      default:
        throw new RuntimeException("Unsupported record mutation type [" + type + "]");
      }
    }
  }

  public void mutate(List<RowMutation> mutations) {
    for (int i = 0; i < mutations.size(); i++) {
      mutate(mutations.get(i));
    }
  }

  public static List<RowMutation> reduceMutates(List<RowMutation> mutations) throws BlurException {
    Map<String, RowMutation> mutateMap = new TreeMap<String, RowMutation>();
    for (RowMutation mutation : mutations) {
      if (mutation.getRowId() == null) {
        throw new BException("Mutation has null rowid [{0}]", mutation);
      }
      RowMutation rowMutation = mutateMap.get(mutation.getRowId());
      if (rowMutation != null) {
        mutateMap.put(mutation.getRowId(), merge(rowMutation, mutation));
      } else {
        mutateMap.put(mutation.getRowId(), mutation);
      }
    }
    return new ArrayList<RowMutation>(mutateMap.values());
  }

  private static RowMutation merge(RowMutation mutation1, RowMutation mutation2) throws BlurException {
    RowMutationType rowMutationType1 = mutation1.getRowMutationType();
    RowMutationType rowMutationType2 = mutation2.getRowMutationType();
    if (!rowMutationType1.equals(rowMutationType2)) {
      throw new BException(
          "RowMutation conflict, cannot perform 2 different operations on the same row in the same batch. [{0}] [{1}]",
          mutation1, mutation2);
    }
    if (rowMutationType1.equals(RowMutationType.DELETE_ROW)) {
      // Since both are trying to delete the same row, just pick one and move
      // on.
      return mutation1;
    } else if (rowMutationType1.equals(RowMutationType.REPLACE_ROW)) {
      throw new BException(
          "RowMutation conflict, cannot perform 2 different REPLACE_ROW mutations on the same row in the same batch. [{0}] [{1}]",
          mutation1, mutation2);
    } else {
      // Now this is a row update, so try to merge the record mutations
      List<RecordMutation> recordMutations1 = mutation1.getRecordMutations();
      List<RecordMutation> recordMutations2 = mutation2.getRecordMutations();
      List<RecordMutation> mergedRecordMutations = merge(recordMutations1, recordMutations2);
      mutation1.setRecordMutations(mergedRecordMutations);
      return mutation1;
    }
  }

  private static List<RecordMutation> merge(List<RecordMutation> recordMutations1, List<RecordMutation> recordMutations2)
      throws BException {
    Map<String, RecordMutation> recordMutationMap = new TreeMap<String, RecordMutation>();
    merge(recordMutations1, recordMutationMap);
    merge(recordMutations2, recordMutationMap);
    return new ArrayList<RecordMutation>(recordMutationMap.values());
  }

  private static void merge(List<RecordMutation> recordMutations, Map<String, RecordMutation> recordMutationMap)
      throws BException {
    for (RecordMutation recordMutation : recordMutations) {
      Record record = recordMutation.getRecord();
      String recordId = record.getRecordId();
      RecordMutation existing = recordMutationMap.get(recordId);
      if (existing != null) {
        recordMutationMap.put(recordId, merge(recordMutation, existing));
      } else {
        recordMutationMap.put(recordId, recordMutation);
      }
    }
  }

  private static RecordMutation merge(RecordMutation recordMutation1, RecordMutation recordMutation2) throws BException {
    RecordMutationType recordMutationType1 = recordMutation1.getRecordMutationType();
    RecordMutationType recordMutationType2 = recordMutation2.getRecordMutationType();
    if (!recordMutationType1.equals(recordMutationType2)) {
      throw new BException(
          "RecordMutation conflict, cannot perform 2 different operations on the same record in the same row in the same batch. [{0}] [{1}]",
          recordMutation1, recordMutation2);
    }

    if (recordMutationType1.equals(RecordMutationType.DELETE_ENTIRE_RECORD)) {
      // Since both are trying to delete the same record, just pick one and move
      // on.
      return recordMutation1;
    } else if (recordMutationType1.equals(RecordMutationType.REPLACE_ENTIRE_RECORD)) {
      throw new BException(
          "RecordMutation conflict, cannot perform 2 different replace record operations on the same record in the same row in the same batch. [{0}] [{1}]",
          recordMutation1, recordMutation2);
    } else if (recordMutationType1.equals(RecordMutationType.REPLACE_COLUMNS)) {
      throw new BException(
          "RecordMutation conflict, cannot perform 2 different replace columns operations on the same record in the same row in the same batch. [{0}] [{1}]",
          recordMutation1, recordMutation2);
    } else {
      Record record1 = recordMutation1.getRecord();
      Record record2 = recordMutation2.getRecord();
      String family1 = record1.getFamily();
      String family2 = record2.getFamily();

      if (isSameFamily(family1, family2)) {
        record1.getColumns().addAll(record2.getColumns());
        return recordMutation1;
      } else {
        throw new BException("RecordMutation conflict, cannot merge records with different family. [{0}] [{1}]",
            recordMutation1, recordMutation2);
      }
    }
  }

  private static boolean isSameFamily(String family1, String family2) {
    if (family1 == null && family2 == null) {
      return true;
    }
    if (family1 != null && family1.equals(family2)) {
      return true;
    }
    return false;
  }
}
