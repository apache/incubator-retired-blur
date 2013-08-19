package org.apache.blur.thrift.util;

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
import java.util.Arrays;

import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.SimpleQuery;

public class BlurThriftHelper {

  public static Selector newSelector(String locationId) {
    Selector selector = new Selector();
    selector.locationId = locationId;
    return selector;
  }

  public static BlurQuery newSimpleQuery(String query) {
    BlurQuery blurQuery = new BlurQuery();
    SimpleQuery simpleQuery = new SimpleQuery();
    simpleQuery.setQuery(query);
    blurQuery.setSimpleQuery(simpleQuery);
    blurQuery.setSelector(new Selector());
    return blurQuery;
  }

  public static RowMutation newRowMutation(String table, String rowId, RecordMutation... mutations) {
    return newRowMutation(RowMutationType.REPLACE_ROW, table, rowId, mutations);
  }

  public static RowMutation newRowMutation(RowMutationType type, String table, String rowId,
      RecordMutation... mutations) {
    RowMutation mutation = new RowMutation();
    mutation.setRowId(rowId);
    mutation.setTable(table);
    mutation.setRowMutationType(type);
    for (RecordMutation recordMutation : mutations) {
      mutation.addToRecordMutations(recordMutation);
    }
    return mutation;
  }

  public static Record newRecord(String family, String recordId, Column... columns) {
    Record record = new Record();
    record.setRecordId(recordId);
    record.setFamily(family);
    record.setColumns(Arrays.asList(columns));
    return record;
  }

  public static Row newRow(String rowId, Record... records) {
    Row row = new Row().setId(rowId);
    for (Record record : records) {
      row.addToRecords(record);
    }
    return row;
  }

  public static Column newColumn(String name, String value) {
    return new Column().setName(name).setValue(value);
  }

  public static RecordMutation newRecordMutation(String family, String recordId, Column... columns) {
    return newRecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, family, recordId, columns);
  }

  public static RecordMutation newRecordMutation(RecordMutationType type, String family, String recordId,
      Column... columns) {
    Record record = new Record();
    record.setRecordId(recordId);
    record.setFamily(family);
    for (Column column : columns) {
      record.addToColumns(column);
    }

    RecordMutation mutation = new RecordMutation();
    mutation.setRecordMutationType(type);
    mutation.setRecord(record);
    return mutation;
  }

  public static RecordMutation findRecordMutation(RowMutation mutation, Record record) {
    for (RecordMutation recordMutation : mutation.recordMutations) {
      if (match(recordMutation, record)) {
        return recordMutation;
      }
    }
    return null;
  }

  public static boolean match(RecordMutation mutation, Record record) {
    return match(mutation.record, record);
  }

  public static boolean match(Record left, Record right) {
    return left.recordId.equals(right.recordId) && left.family.equals(right.family);
  }

}
