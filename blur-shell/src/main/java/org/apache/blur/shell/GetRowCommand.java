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
import java.util.List;

import jline.Terminal;
import jline.console.ConsoleReader;

import org.apache.blur.shell.PagingPrintWriter.FinishedException;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.Selector;

public class GetRowCommand extends Command {

  @Override
  public void doit(PrintWriter outPw, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length != 3) {
      throw new CommandException("Invalid args: " + help());
    }
    
    PagingPrintWriter out = new PagingPrintWriter(outPw);
    
    try {
      doItInternal(client, args, out);
    } catch (FinishedException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
    }
  }

  private void doItInternal(Blur.Iface client, String[] args, PagingPrintWriter out) throws BlurException, TException,
      FinishedException {
    String tablename = args[1];
    String rowId = args[2];

    Selector selector = new Selector(Main.selector);
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
    int maxWidth = 100;
    ConsoleReader reader = getConsoleReader();
    if (reader != null) {
      Terminal terminal = reader.getTerminal();
      maxWidth = terminal.getWidth() - 15;
      out.setLineLimit(terminal.getHeight() - 2);
    }
    format(out, row, maxWidth);
  }

  public static void format(PagingPrintWriter out, Row row, int maxWidth) throws FinishedException {
    String id = row.getId();
    int recordCount = row.getRecordCount();
    out.println("       id : " + id);
    if (Main.debug) {
      out.println("recordCount : " + recordCount);
    }
    List<Record> records = row.getRecords();
    for (Record record : records) {
      format(out, record, maxWidth);
    }
  }

  public static void format(PagingPrintWriter out, Record record, int maxWidth) throws FinishedException {
    String recordId = record.getRecordId();
    String family = record.getFamily();
    List<Column> columns = record.getColumns();
    out.println(" recordId : " + recordId);
    out.println("   family : " + family);
    for (Column column : columns) {
      format(out, column, maxWidth);
    }
  }

  private static void format(PagingPrintWriter out, Column column, int maxWidth) throws FinishedException {
    String lead = "     " + column.getName() + " : ";
    String value = column.getValue();
    int length = value.length();
    int position = 0;
    while (length > 0) {
      int len = Math.min(maxWidth, length);
      String s = value.substring(position, position + len);
      if (position == 0) {
        out.println(lead + s);
      } else {
        for (int o = 0; o < lead.length(); o++) {
          out.print(' ');
        }
        out.println(s);
      }
      position += len;
      length -= len;
    }

  }

  @Override
  public String help() {
    return "display the specified row, args; tablename rowid";
  }
}
