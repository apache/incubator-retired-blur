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
import java.text.NumberFormat;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableStats;

public class TableStatsCommand extends Command implements TableFirstArgCommand {
  private static final double _1KB = 1000;
  private static final double _1MB = _1KB * 1000;
  private static final double _1GB = _1MB * 1000;
  private static final double _1TB = _1GB * 1000;
  private static final double _1PB = _1TB * 1000;

  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length != 2) {
      throw new CommandException("Invalid args: " + help());
    }
    String tablename = args[1];

    TableStats tableStats = client.tableStats(tablename);
    long bytes = tableStats.getBytes();
    long recordCount = tableStats.getRecordCount();
    long rowCount = tableStats.getRowCount();
    long segmentImportInProgressCount = tableStats.getSegmentImportInProgressCount();
    long segmentImportPendingCount = tableStats.getSegmentImportPendingCount();
    // Queries is an unknown value now.
    out.println("Row Count           : " + numberFormat(rowCount));
    out.println("Record Count        : " + numberFormat(recordCount));
    out.println("Table Size          : " + humanize(bytes));
    out.println("Pending Imports     : " + segmentImportPendingCount);
    out.println("In Progress Imports : " + segmentImportInProgressCount);
  }

  private String numberFormat(long l) {
    return NumberFormat.getNumberInstance().format(l);
  }

  private String humanize(long bytes) {
    double result = bytes / _1PB;
    if (((long) result) > 0) {
      return result + " PB";
    }
    result = bytes / _1TB;
    if (((long) result) > 0) {
      return result + " TB";
    }
    result = bytes / _1GB;
    if (((long) result) > 0) {
      return result + " GB";
    }
    result = bytes / _1MB;
    if (((long) result) > 0) {
      return result + " MB";
    }
    result = bytes / _1KB;
    if (((long) result) > 0) {
      return result + " KB";
    }
    return result + " Bytes";
  }

  @Override
  public String description() {
    return "Print stats for the named table.";
  }

  @Override
  public String usage() {
    return "<tablename>";
  }

  @Override
  public String name() {
    return "stats";
  }
}
