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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.commons.cli.CommandLine;

public class QueryCommand extends Command implements TableFirstArgCommand {

  static enum RenderType {
    ROW_MULTI_FAMILY, ROW_SINGLE_FAMILY
  }

  private int _width;

  @Override
  public void doit(PrintWriter outPw, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length < 3) {
      throw new CommandException("Invalid args: " + help());
    }

    try {
      doItInternal(client, args, outPw);
    } catch (FinishedException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
    }
  }

  private void doItInternal(Blur.Iface client, String[] args, PrintWriter out) throws FinishedException, BlurException,
      TException {
    String tablename = args[1];
    CommandLine commandLine = QueryCommandHelper.parse(args, out);
    if (commandLine == null) {
      return;
    }
    BlurQuery blurQuery = QueryCommandHelper.getBlurQuery(commandLine);
    if (Main.debug) {
      out.println(blurQuery);
    }
    _width = 100;
    if (commandLine.hasOption(QueryCommandHelper.WIDTH)) {
      _width = Integer.parseInt(commandLine.getOptionValue(QueryCommandHelper.WIDTH));
    }
    ConsoleReader reader = getConsoleReader();
    if (reader == null) {
      throw new BlurException("This command can only be run with a proper jline environment.", null, ErrorType.UNKNOWN);
    }

    long s = System.nanoTime();
    BlurResults blurResults = client.query(tablename, blurQuery);
    long e = System.nanoTime();
    long timeInNanos = e - s;
    if (blurResults.getTotalResults() == 0) {
      out.println("No results found in [" + timeInNanos / 1000000.0 + " ms].");
      return;
    }

    String prompt = reader.getPrompt();
    reader.setPrompt("");
    final TableDisplay tableDisplay = new TableDisplay(reader);
    tableDisplay.setDescription(white(blurResults.getTotalResults() + " results found in [" + timeInNanos / 1000000.0
        + " ms]."));
    tableDisplay.setSeperator(" ");
    try {

      final AtomicBoolean viewing = new AtomicBoolean(true);

      tableDisplay.addKeyHook(new Runnable() {
        @Override
        public void run() {
          synchronized (viewing) {
            viewing.set(false);
            viewing.notify();
            tableDisplay.setStopReadingInput(true);
          }
        }
      }, 'q');

      RenderType type = getRenderRype(blurResults);
      switch (type) {
      case ROW_MULTI_FAMILY:
        renderRowMultiFamily(tableDisplay, blurResults);
        break;
      case ROW_SINGLE_FAMILY:
        renderRowSingleFamily(tableDisplay, blurResults);
        break;
      default:
        break;
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
      try {
        tableDisplay.close();
      } catch (IOException ex) {
        if (Main.debug) {
          ex.printStackTrace();
        }
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        if (Main.debug) {
          ex.printStackTrace();
        }
      }
      try {
        reader.setPrompt("");
        reader.clearScreen();
      } catch (IOException ex) {
        if (Main.debug) {
          ex.printStackTrace();
        }
      }
      out.write("\u001B[0m");
      out.flush();
      reader.setPrompt(prompt);
    }
  }

  private RenderType getRenderRype(BlurResults blurResults) {
    Set<String> families = new HashSet<String>();
    for (BlurResult blurResult : blurResults.getResults()) {
      families.addAll(getFamily(blurResult.getFetchResult()));
    }
    if (families.size() > 1) {
      return RenderType.ROW_MULTI_FAMILY;
    }
    return RenderType.ROW_SINGLE_FAMILY;
  }

  private Set<String> getFamily(FetchResult fetchResult) {
    Set<String> result = new HashSet<String>();
    FetchRowResult rowResult = fetchResult.getRowResult();
    if (rowResult == null) {
      FetchRecordResult recordResult = fetchResult.getRecordResult();
      Record record = recordResult.getRecord();
      result.add(record.getFamily());
    } else {
      Row row = rowResult.getRow();
      List<Record> records = row.getRecords();
      if (records != null) {
        for (Record record : records) {
          result.add(record.getFamily());
        }
      }
    }
    return result;
  }

  private void renderRowSingleFamily(TableDisplay tableDisplay, BlurResults blurResults) {
    int line = 0;
    tableDisplay.setHeader(0, highlight(getTruncatedVersion("rowid")));
    tableDisplay.setHeader(1, highlight(getTruncatedVersion("recordid")));
    List<String> columnsLabels = new ArrayList<String>();
    for (BlurResult blurResult : blurResults.getResults()) {
      FetchResult fetchResult = blurResult.getFetchResult();
      FetchRowResult rowResult = fetchResult.getRowResult();
      if (rowResult == null) {
        FetchRecordResult recordResult = fetchResult.getRecordResult();
        String rowid = recordResult.getRowid();
        Record record = recordResult.getRecord();
        String family = record.getFamily();
        if (record.getColumns() != null) {
          for (Column column : record.getColumns()) {
            addToTableDisplay(columnsLabels, tableDisplay, line, rowid, family, record.getRecordId(), column);
          }
        }
        line++;
      } else {
        Row row = rowResult.getRow();
        List<Record> records = row.getRecords();
        if (records != null) {
          for (Record record : records) {
            if (record.getColumns() != null) {
              for (Column column : record.getColumns()) {
                addToTableDisplay(columnsLabels, tableDisplay, line, row.getId(), record.getFamily(),
                    record.getRecordId(), column);
              }
            }
            line++;
          }
        }
      }
    }
  }

  private void addToTableDisplay(List<String> columnsLabels, TableDisplay tableDisplay, int line, String rowId,
      String family, String recordId, Column column) {
    String name = family + "." + column.getName();
    int indexOf = columnsLabels.indexOf(name);
    if (indexOf < 0) {
      indexOf = columnsLabels.size();
      columnsLabels.add(name);
      tableDisplay.setHeader(indexOf + 2, highlight(getTruncatedVersion(name)));
    }
    tableDisplay.set(0, line, white(getTruncatedVersion(rowId)));
    tableDisplay.set(1, line, white(getTruncatedVersion(recordId)));
    tableDisplay.set(indexOf + 2, line, white(getTruncatedVersion(column.getValue())));
  }

  private String getTruncatedVersion(String s) {
    if (s.length() > _width) {
      return s.substring(0, _width - 3) + "...";
    }
    return s;
  }

  private void renderRowMultiFamily(TableDisplay tableDisplay, BlurResults blurResults) {
    AtomicInteger line = new AtomicInteger();
    tableDisplay.setHeader(0, highlight(getTruncatedVersion("rowid")));
    tableDisplay.setHeader(1, highlight(getTruncatedVersion("recordid")));
    Map<String, List<String>> columnOrder = new HashMap<String, List<String>>();
    for (BlurResult result : blurResults.getResults()) {
      FetchResult fetchResult = result.getFetchResult();
      FetchRowResult rowResult = fetchResult.getRowResult();
      if (rowResult != null) {
        Row row = rowResult.getRow();
        String id = row.getId();
        tableDisplay.set(0, line.get(), white(getTruncatedVersion(id)));
        List<Record> records = order(row.getRecords());
        String currentFamily = "#";
        for (Record record : records) {
          currentFamily = displayRecordInRowMultiFamilyView(tableDisplay, line, columnOrder, currentFamily, record);
        }
      } else {
        String currentFamily = "#";
        FetchRecordResult recordResult = fetchResult.getRecordResult();
        Record record = recordResult.getRecord();
        currentFamily = displayRecordInRowMultiFamilyView(tableDisplay, line, columnOrder, currentFamily, record);
      }
    }
  }

  private String displayRecordInRowMultiFamilyView(final TableDisplay tableDisplay, final AtomicInteger line,
      final Map<String, List<String>> columnOrder, final String currentFamily, final Record record) {
    int c = 2;
    List<String> orderedColumns = getOrderColumnValues(record, columnOrder);
    String family = record.getFamily();
    if (!family.equals(currentFamily)) {
      List<String> list = columnOrder.get(family);
      for (int i = 0; i < list.size(); i++) {
        tableDisplay.set(i + c, line.get(), highlight(getTruncatedVersion(family + "." + list.get(i))));
      }
      line.incrementAndGet();
    }
    tableDisplay.set(1, line.get(), white(getTruncatedVersion(record.getRecordId())));
    for (String oc : orderedColumns) {
      if (oc != null) {
        tableDisplay.set(c, line.get(), white(getTruncatedVersion(oc)));
      }
      c++;
    }
    line.incrementAndGet();
    return family;
  }

  private String white(String s) {
    return "\u001B[0m" + s;
  }

  private String highlight(String s) {
    return "\u001B[33m" + s;
  }

  private List<Record> order(List<Record> records) {
    List<Record> list = new ArrayList<Record>(records);
    Collections.sort(list, new Comparator<Record>() {
      @Override
      public int compare(Record o1, Record o2) {
        String family1 = o1.getFamily();
        String family2 = o2.getFamily();
        String recordId1 = o1.getRecordId();
        String recordId2 = o2.getRecordId();
        if (family1 == null && family2 == null) {
          return recordId1.compareTo(recordId2);
        }
        if (family1 == null) {
          return -1;
        }
        int compareTo = family1.compareTo(family2);
        if (compareTo == 0) {
          return recordId1.compareTo(recordId2);
        }
        return compareTo;
      }
    });
    return list;
  }

  private List<String> getOrderColumnValues(Record record, Map<String, List<String>> columnOrder) {
    String family = record.getFamily();
    List<String> columnNameList = columnOrder.get(family);
    if (columnNameList == null) {
      columnOrder.put(family, columnNameList = new ArrayList<String>());
    }
    Map<String, List<Column>> columnMap = getColumnMap(record);
    Set<String> recordColumnNames = new TreeSet<String>(columnMap.keySet());
    for (String cn : recordColumnNames) {
      if (!columnNameList.contains(cn)) {
        columnNameList.add(cn);
      }
    }
    List<String> result = new ArrayList<String>();
    for (String col : columnNameList) {
      List<Column> list = columnMap.get(col);
      if (list != null) {
        result.add(toString(list));
      } else {
        result.add(null);
      }
    }
    return result;
  }

  private String toString(List<Column> list) {
    if (list.size() == 0) {
      throw new RuntimeException("Should not happen");
    }
    if (list.size() == 1) {
      return list.get(0).getValue();
    }
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < list.size(); i++) {
      Column column = list.get(i);
      if (i != 0) {
        builder.append(",");
      }
      builder.append("[").append(column.getValue()).append("]");
    }
    return builder.toString();
  }

  private Map<String, List<Column>> getColumnMap(Record record) {
    Map<String, List<Column>> map = new HashMap<String, List<Column>>();
    for (Column column : record.getColumns()) {
      String name = column.getName();
      List<Column> list = map.get(name);
      if (list == null) {
        map.put(name, list = new ArrayList<Column>());
      }
      list.add(column);
    }
    return map;
  }

  @Override
  public String description() {
    return "Query the named table.  Run -h for full argument list.";
  }

  @Override
  public String usage() {
    return "<tablename> <query> [<options>]";
  }

  @Override
  public String name() {
    return "query";
  }
}
