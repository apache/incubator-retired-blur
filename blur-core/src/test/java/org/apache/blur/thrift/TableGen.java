package org.apache.blur.thrift;
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
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.TableDescriptor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TableGen {
  private static final String NUM_PATTERN = "###";
  private static final int RECORD_ID_COUNTER = -1;
  private String table;
  private int shardCount = 1;
  private String uri;
  private List<ColumnDefinition> cols = Lists.newArrayList();
  private Map<String, List<RecordMutation>> vals = Maps.newHashMap();

  private TableGen(String tableName) {
    this.table = tableName;
  }

  public static TableGen define(String name) {
    return new TableGen(name);
  }

  public TableGen at(String uri) {
    this.uri = uri;
    return this;
  }

  public TableGen cols(String family, String... columns) {
    for (String c : columns) {
      ColumnDefinition cd = new ColumnDefinition();
      String colName = c;

      if (c.contains(":")) {
        String[] colInfo = c.split(":");
        cd.setFieldType(colInfo[0]);
        colName = colInfo[1];
      }

      cd.setFamily(family);
      cd.setColumnName(colName);
      cols.add(cd);
    }

    return this;
  }
  
  public TableGen addRows(int rowCount, int recordCount, String row, String rec, Object... values) {
    
    for(int i = 0; i < rowCount; i++) {
      String rowId = row + "-" + i;
      if(row.contains(NUM_PATTERN)) {
        rowId = row.replace(NUM_PATTERN, Integer.toString(i));
      }
      addRow(recordCount, rowId, rec, values);
    }
    
    return this;
  }

  public TableGen addRow(int number, String row, String rec, Object... values) {
    Map<Integer, Integer> counters = Maps.newHashMap();
    counters.put(RECORD_ID_COUNTER, 0);

    for (int i = 0; i < values.length; i++) {
      if (values[i].toString().contains(NUM_PATTERN)) {
        counters.put(i, 0);
      }
    }

    for (int i = 0; i < number; i++) {
      Object[] genValues = new Object[values.length];
      for (int j = 0; j < values.length; j++) {
        Integer counter = counters.get(j);
        if (counter != null) {
          genValues[j] = ((String) values[j]).replace(NUM_PATTERN, Integer.toString(counter));
          counters.put(j, counter + 1);
        } else {
          genValues[j] = values[j].toString();
        }
      }
      int recordCounter = counters.get(-1);
      addRecord(row, rec.replace(NUM_PATTERN, Integer.toString(recordCounter)), genValues);
      counters.put(RECORD_ID_COUNTER, recordCounter + 1);
    }
    
    return this;
  }

  public TableGen addRecord(String row, String rec, Object... values) {
    List<RecordMutation> records = vals.get(row);
    if (records == null) {
      records = Lists.newArrayList();
      vals.put(row, records);
    }

    Record record = new Record();
    record.setRecordId(rec);

    for (int i = 0; i < cols.size(); i++) {
      ColumnDefinition cd = cols.get(i);

      record.setFamily(cd.getFamily());
      record.addToColumns(new Column(cd.getColumnName(), values[i].toString()));
    }
    
    records.add(new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, record));

    return this;
  }

  public void build(Iface client) throws BlurException, TException, IOException {
    TableDescriptor tbl = new TableDescriptor();
    tbl.setName(table);
    tbl.setShardCount(shardCount);
    if (uri == null) {
      tbl.setTableUri(SuiteCluster.getFileSystemUri().toString() + "/blur/" + table);
    } else {
      tbl.setTableUri(uri);
    }
    client.createTable(tbl);

    for (String rowId : vals.keySet()) {
      client.mutate(new RowMutation(table, rowId, RowMutationType.REPLACE_ROW, vals.get(rowId)));
    }
  }

}
