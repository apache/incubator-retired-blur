/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.blur.shell;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;

public class MutateRowCommand extends Command implements TableFirstArgCommand {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length < 6) {
      throw new CommandException("Invalid args: " + help());
    }
    String tablename = args[1];
    String rowid = args[2];
    String recordid = args[3];
    String columnfamily = args[4];

    List<Column> columns = new ArrayList<Column>();
    for (int i = 5; i < args.length; i++) {
      String namePlusValue = args[i];
      int index = namePlusValue.indexOf(":");
      String name = namePlusValue.substring(0, index);
      String value = namePlusValue.substring(index + 1);
      columns.add(new Column(name, value));
    }

    Record record = new Record();
    record.setRecordId(recordid);
    record.setFamily(columnfamily);
    record.setColumns(columns);

    RecordMutation recordMutation = new RecordMutation();
    recordMutation.setRecord(record);
    recordMutation.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);

    List<RecordMutation> recordMutations = new ArrayList<RecordMutation>();
    recordMutations.add(recordMutation);

    RowMutation mutation = new RowMutation();
    mutation.setTable(tablename);
    mutation.setRowId(rowid);
    mutation.setRowMutationType(RowMutationType.UPDATE_ROW);
    mutation.setRecordMutations(recordMutations);

    client.mutate(mutation);
  }

  @Override
  public String description() {
    return "Mutate the specified row.";
  }

  @Override
  public String usage() {
    return "<tablename> <rowid> <recordid> <columnfamily> <columnname>:<value>*";
  }

  @Override
  public String name() {
    return "mutate";
  }
}
