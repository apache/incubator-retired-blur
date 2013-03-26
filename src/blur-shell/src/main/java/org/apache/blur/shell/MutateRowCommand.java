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

import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.thrift.TException;

public class MutateRowCommand extends Command {
  @Override
  public void doit(PrintWriter out, Client client, String[] args)
      throws CommandException, TException, BlurException {
    if (args.length != 7) {
      throw new CommandException("Invalid args: " + help());
    }
    String tablename = args[1];
    String rowid = args[2];
    String recordid = args[3];
    String columnfamily = args[4];
    String columnname = args[5];
    String value = args[6];

    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(columnname, value));

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
    mutation.setRowMutationType(RowMutationType.REPLACE_ROW);
    mutation.setRecordMutations(recordMutations);

    client.mutate(mutation);
  }

  @Override
  public String help() {
    return "mutate the specified row, args; tablename rowid recordid columnfamily columnname value";
  }
}
