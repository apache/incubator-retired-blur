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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.manager.IndexManager;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.RowDocumentUtil;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

public class MutatableAction {

  static class UpdateRow extends InternalAction {

    static abstract class UpdateRowAction {
      abstract Row performAction(Row row);
    }

    private final List<UpdateRowAction> _actions = new ArrayList<UpdateRowAction>();
    private final String _rowId;
    private final String _table;
    private final String _shard;
    private final int _maxHeap;
    private final TableContext _tableContext;
    private final FieldManager _fieldManager;

    UpdateRow(String rowId, String table, String shard, int maxHeap, TableContext tableContext) {
      _rowId = rowId;
      _table = table;
      _shard = shard;
      _maxHeap = maxHeap;
      _tableContext = tableContext;
      _fieldManager = _tableContext.getFieldManager();
    }

    void deleteRecord(final String recordId) {
      _actions.add(new UpdateRowAction() {
        @Override
        Row performAction(Row row) {
          if (row == null) {
            return null;
          } else {
            if (row.getRecords() == null) {
              return row;
            }
            Row result = new Row();
            result.setId(row.getId());
            for (Record record : row.getRecords()) {
              if (!record.getRecordId().equals(recordId)) {
                result.addToRecords(record);
              }
            }
            return result;
          }
        }
      });
    }

    void appendColumns(final Record record) {
      _actions.add(new UpdateRowAction() {
        @Override
        Row performAction(Row row) {
          if (row == null) {
            row = new Row(_rowId, null);
            row.addToRecords(record);
            return row;
          } else {
            Row result = new Row();
            result.setId(row.getId());
            String recordId = record.getRecordId();
            boolean found = false;
            if (row.getRecords() != null) {
              for (Record r : row.getRecords()) {
                if (!r.getRecordId().equals(recordId)) {
                  result.addToRecords(r);
                } else {
                  found = true;
                  // Append columns
                  r.getColumns().addAll(record.getColumns());
                  result.addToRecords(r);
                }
              }
            }
            if (!found) {
              result.addToRecords(record);
            }
            return result;
          }
        }
      });
    }

    void replaceColumns(final Record record) {
      _actions.add(new UpdateRowAction() {
        @Override
        Row performAction(Row row) {
          if (row == null) {
            row = new Row(_rowId, null);
            row.addToRecords(record);
            return row;
          } else {
            Row result = new Row();
            result.setId(row.getId());
            String recordId = record.getRecordId();
            boolean found = false;
            if (row.getRecords() != null) {
              for (Record r : row.getRecords()) {
                if (!r.getRecordId().equals(recordId)) {
                  result.addToRecords(r);
                } else {
                  found = true;
                  // Replace columns
                  result.addToRecords(replaceColumns(r, record));
                }
              }
            }
            if (!found) {
              result.addToRecords(record);
            }
            return result;
          }
        }
      });
    }

    protected Record replaceColumns(Record existing, Record newRecord) {
      Map<String, List<Column>> existingColumns = getColumnMap(existing.getColumns());
      Map<String, List<Column>> newColumns = getColumnMap(newRecord.getColumns());
      existingColumns.putAll(newColumns);
      Record record = new Record();
      record.setFamily(existing.getFamily());
      record.setRecordId(existing.getRecordId());
      record.setColumns(toList(existingColumns.values()));
      return record;
    }

    private List<Column> toList(Collection<List<Column>> values) {
      ArrayList<Column> list = new ArrayList<Column>();
      for (List<Column> v : values) {
        list.addAll(v);
      }
      return list;
    }

    private Map<String, List<Column>> getColumnMap(List<Column> columns) {
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
      _actions.add(new UpdateRowAction() {
        @Override
        Row performAction(Row row) {
          if (row == null) {
            row = new Row(_rowId, null);
            row.addToRecords(record);
            return row;
          } else {
            Row result = new Row();
            result.setId(row.getId());
            String recordId = record.getRecordId();
            if (row.getRecords() != null) {
              for (Record r : row.getRecords()) {
                if (!r.getRecordId().equals(recordId)) {
                  result.addToRecords(r);
                }
              }
            }
            // Add replacement
            result.addToRecords(record);
            return result;
          }
        }
      });
    }

    @Override
    void performAction(IndexSearcherClosable searcher, IndexWriter writer) throws IOException {
      Selector selector = new Selector();
      selector.setRowId(_rowId);
      IndexManager.populateSelector(searcher, _shard, _table, selector);
      Row row = null;
      if (!selector.getLocationId().equals(IndexManager.NOT_FOUND)) {
        FetchResult fetchResult = new FetchResult();
        IndexManager.fetchRow(searcher.getIndexReader(), _table, _shard, selector, fetchResult, null, null, _maxHeap, _tableContext, null);
        FetchRowResult rowResult = fetchResult.getRowResult();
        if (rowResult != null) {
          row = rowResult.getRow();
        }
      }
      for (UpdateRowAction action : _actions) {
        row = action.performAction(row);
      }
      Term term = createRowId(_rowId);
      if (row != null && row.getRecords() != null && row.getRecords().size() > 0) {
        List<List<Field>> docsToUpdate = RowDocumentUtil.getDocs(row, _fieldManager);
        writer.updateDocuments(term, docsToUpdate);
      } else {
        writer.deleteDocuments(term);
      }
    }

  }

  static abstract class InternalAction {
    abstract void performAction(IndexSearcherClosable searcher, IndexWriter writer) throws IOException;
  }

  private final List<InternalAction> _actions = new ArrayList<InternalAction>();
  private final Map<String, UpdateRow> _rowUpdates = new HashMap<String, UpdateRow>();
  private final FieldManager _fieldManager;
  private final String _shard;
  private final String _table;
  private final int _maxHeap = Integer.MAX_VALUE;
  private TableContext _tableContext;

  public MutatableAction(ShardContext context) {
    _tableContext = context.getTableContext();
    _shard = context.getShard();
    _table = _tableContext.getTable();
    _fieldManager = _tableContext.getFieldManager();
  }

  public void deleteRow(final String rowId) {
    _actions.add(new InternalAction() {
      @Override
      void performAction(IndexSearcherClosable searcher, IndexWriter writer) throws IOException {
        writer.deleteDocuments(createRowId(rowId));
      }
    });
  }

  public void replaceRow(final Row row) {
    _actions.add(new InternalAction() {
      @Override
      void performAction(IndexSearcherClosable searcher, IndexWriter writer) throws IOException {
        List<List<Field>> docs = RowDocumentUtil.getDocs(row, _fieldManager);
        Term rowId = createRowId(row.getId());
        writer.updateDocuments(rowId, docs);
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

  void performMutate(IndexSearcherClosable searcher, IndexWriter writer) throws IOException {
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
      updateRow = new UpdateRow(rowId, _table, _shard, _maxHeap, _tableContext);
      _rowUpdates.put(rowId, updateRow);
      _actions.add(updateRow);
    }
    return updateRow;
  }

}
