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

import jline.Terminal;
import jline.console.ConsoleReader;

import org.apache.blur.shell.PagingPrintWriter.FinishedException;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.HighlightOptions;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Selector;

public class QueryCommand extends Command implements TableFirstArgCommand {
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

    int maxWidth = 100;
    ConsoleReader reader = getConsoleReader();
    if (reader != null) {
      Terminal terminal = reader.getTerminal();
      maxWidth = terminal.getWidth() - 15;
      out.setLineLimit(terminal.getHeight() - 2);
    }
    long s = System.nanoTime();
    BlurResults blurResults = client.query(tablename, blurQuery);
    long e = System.nanoTime();
    long timeInNanos = e - s;

    printSummary(out, blurResults, maxWidth, timeInNanos);
    lineBreak(out, maxWidth);

    int hit = 0;
    for (BlurResult result : blurResults.getResults()) {
      double score = result.getScore();
      out.println("      hit : " + hit);
      out.println("    score : " + score);
      if (Main.debug) {
        String locationId = result.getLocationId();
        out.println("locationId : " + locationId);
      }
      FetchResult fetchResult = result.getFetchResult();
      if (Main.debug) {
        out.println("deleted : " + fetchResult.isDeleted());
        out.println("exists  : " + fetchResult.isExists());
        out.println("table   : " + fetchResult.getTable());
      }
      FetchRowResult rowResult = fetchResult.getRowResult();
      if (rowResult != null) {
        Row row = rowResult.getRow();
        if (row != null) {
          GetRowCommand.format(out, row, maxWidth);
        }
      }
      lineBreak(out, maxWidth);
      hit++;
    }
    printSummary(out, blurResults, maxWidth, timeInNanos);
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
    return "query";
  }
}
