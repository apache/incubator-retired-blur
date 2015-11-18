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
package org.apache.blur.thrift;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.Selector;

public class BlurMutationBug {

  private final Iface client;
  private final String tableName;
  private final String[] rowIds;

  public static void main(String[] args) throws TException {
    Iface client = BlurClient.getClient("localhost:40010");
    BlurMutationBug blurMutationBug = new BlurMutationBug(client, "testtable", 20);
    blurMutationBug.runTest(5, TimeUnit.MINUTES);
  }

  public BlurMutationBug(Iface client, String tableName, int totalIds) {
    this.client = client;
    this.tableName = tableName;
    rowIds = new String[totalIds];
    for (int i = 0; i < totalIds; i++) {
      rowIds[i] = UUID.randomUUID().toString();
    }

  }

  public void runTest(long totalTime, TimeUnit timeUnit) throws TException {
    long testTime = timeUnit.toMillis(totalTime);
    long startTime = System.currentTimeMillis();
    while (testTime + startTime >= System.currentTimeMillis()) {
      for (String rowId : rowIds) {
        addMetaToBlur(rowId);
      }
    }
  }

  private void addMetaToBlur(String rowId) throws TException {
    RowMutation rowMutation = buildMeta(rowId);
    mutateIndex(rowMutation);
    Row row = fetchRow(rowMutation.getRowId());
    if ((row == null) || !row.getId().equals(rowMutation.getRowId())) {
      throw new IllegalStateException("Fetch of rowId " + rowMutation.getRowId() + " returned wrong rowId "
          + ((row == null) ? "null" : row.getId()));
    }
    checkForCorruption();
  }

  private void checkForCorruption() throws TException {
    for (String rowId : rowIds) {
      Row row = fetchRow(rowId);
      if (row != null) {
        List<String> metaKeys = new ArrayList<String>(1);
        for (Record record : row.getRecords()) {
          for (Column column : record.getColumns()) {
            if ("key".equals(column.getName())) {
              metaKeys.add(column.getValue());
            }
          }
        }
        if ((metaKeys.size() != 1) || !metaKeys.get(0).equals(rowId)) {
          throw new IllegalStateException("corrupt row with bad key(s) " + metaKeys);
        }
      }
    }
  }

  private RowMutation buildMeta(String rowId) {
    Column column = new Column();
    column.setName("key");
    column.setValue(rowId);
    List<Column> columns = new ArrayList<Column>();
    columns.add(column);

    Record record = new Record();
    record.setFamily("meta");
    record.setColumns(columns);
    record.setRecordId("0");

    List<RecordMutation> recMutations = new ArrayList<RecordMutation>(1);
    RecordMutation recMutation = new RecordMutation();
    recMutation.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);
    recMutation.setRecord(record);
    recMutations.add(recMutation);

    RowMutation rowMutation = new RowMutation();
    rowMutation.setTable(tableName);
    rowMutation.setRowId(rowId);
    rowMutation.setRowMutationType(RowMutationType.REPLACE_ROW);
    rowMutation.setRecordMutations(recMutations);

    return (rowMutation);
  }

  private void mutateIndex(RowMutation rowMutation) throws TException {
    Iface blurClient = getClient();
    blurClient.mutate(rowMutation);
  }

  private Row fetchRow(String rowId) throws TException {
    Iface blurClient = getClient();
    Selector selector = new Selector();
    selector.setRowId(rowId);

    FetchResult fetchResult = blurClient.fetchRow(tableName, selector);
    Row row = null;
    if (fetchResult != null && fetchResult.isExists() && fetchResult.isSetRowResult()) {
      row = fetchResult.getRowResult().getRow();
    }
    return (row);
  }

  private Iface getClient() {
    return client;
  }
}