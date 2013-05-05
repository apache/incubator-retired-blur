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

import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.Selector;
import org.apache.thrift.TException;

public class GetRowCommand extends Command {
  @Override
  public void doit(PrintWriter out, Client client, String[] args) throws CommandException, TException, BlurException {
    if (args.length != 3) {
      throw new CommandException("Invalid args: " + help());
    }
    String tablename = args[1];
    String rowId = args[2];

    Selector selector = new Selector();
    selector.setRowId(rowId);
    FetchResult fetchRow = client.fetchRow(tablename, selector);
    FetchRowResult rowResult = fetchRow.getRowResult();
    if (rowResult == null) {
      out.println("Row [" + rowId + "] not found.");
      return;
    }
    Row row = rowResult.getRow();
    if (row == null) {
      out.println("Row [" + rowId + "] not found.");
      return;
    }
    out.println(row);
  }

  @Override
  public String help() {
    return "display the specified row, args; tablename query";
  }
}
