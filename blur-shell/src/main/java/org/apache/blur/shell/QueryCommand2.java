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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import jline.Terminal;
import jline.console.ConsoleReader;

import org.apache.blur.shell.PagingPrintWriter.FinishedException;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.HighlightOptions;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Selector;

public class QueryCommand2 extends Command implements TableFirstArgCommand {
  @Override
  public void doit(PrintWriter outPw, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length < 3) {
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

  private void doItInternal(Blur.Iface client, String[] args, PagingPrintWriter out) throws FinishedException,
      BlurException, TException {
    String tablename = args[1];
    String queryStr = "";
    for (int i = 2; i < args.length; i++) {
      queryStr += args[i] + " ";
    }

    BlurQuery blurQuery = new BlurQuery();
    Query query = new Query();
    query.setQuery(queryStr);
    blurQuery.setQuery(query);
    blurQuery.setSelector(new Selector(Main.selector));
    blurQuery.setCacheResult(false);
    blurQuery.setUseCacheIfPresent(false);

    if (Main.highlight) {
      blurQuery.getSelector().setHighlightOptions(new HighlightOptions());
    }

    if (Main.debug) {
      out.println(blurQuery);
    }

    ConsoleReader reader = getConsoleReader();
    if (reader == null) {
      throw new BlurException("This command can only be run with a proper jline environment.", null, ErrorType.UNKNOWN);
    }
    String prompt = reader.getPrompt();
    reader.setPrompt("");
    TableDisplay tableDisplay = new TableDisplay(reader);
    tableDisplay.setSeperator(" ");
    try {

      long s = System.nanoTime();
      BlurResults blurResults = client.query(tablename, blurQuery);
      long e = System.nanoTime();
      long timeInNanos = e - s;

      final AtomicBoolean viewing = new AtomicBoolean(true);

      tableDisplay.addKeyHook(new Runnable() {
        @Override
        public void run() {
          synchronized (viewing) {
            viewing.set(false);
            viewing.notify();
          }
        }
      }, 'q');

      int line = 0;

      tableDisplay.setHeader(0, "rowid");
      tableDisplay.setHeader(1, "family");
      tableDisplay.setHeader(2, "recordid");

      for (BlurResult result : blurResults.getResults()) {
        FetchResult fetchResult = result.getFetchResult();
        FetchRowResult rowResult = fetchResult.getRowResult();
        if (rowResult != null) {
          Row row = rowResult.getRow();
          String id = row.getId();
          List<Record> records = row.getRecords();
          for (Record record : records) {
            tableDisplay.set(0, line, id);
            tableDisplay.set(1, line, record.getFamily());
            tableDisplay.set(2, line, record.getRecordId());
            int c = 3;
            for (Column column : record.getColumns()) {
              tableDisplay.set(c, line, column.getName() + " " + column.getValue());
              c++;
            }
            line++;
          }
        } else {
          throw new RuntimeException("impl");
        }
      }
      while (viewing.get()) {
        synchronized (viewing) {
          try {
            viewing.wait();
          } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
    } finally {
      reader.setPrompt(prompt);
      try {
        tableDisplay.close();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
  }

  private void lineBreak(PagingPrintWriter out, int maxWidth) throws FinishedException {
    for (int i = 0; i < maxWidth; i++) {
      out.print('-');
    }
    out.println();
  }

  private void printSummary(PagingPrintWriter out, BlurResults blurResults, int maxWidth, long timeInNanos)
      throws FinishedException {
    long totalResults = blurResults.getTotalResults();
    out.println(" - Results Summary -");
    out.println("    total : " + totalResults);
    out.println("    time  : " + (timeInNanos / 1000000.0) + " ms");
    if (Main.debug) {
      out.println("shardinfo: " + blurResults.getShardInfo());
    }
  }

  @Override
  public String description() {
    return "Query the named table.";
  }

  @Override
  public String usage() {
    return "<tablename> <query>";
  }

  @Override
  public String name() {
    return "query2";
  }
}
