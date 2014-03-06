package org.apache.blur.shell;

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
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.util.LoadData;

public class LoadTestDataCommand extends Command implements TableFirstArgCommand {

  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length != 9) {
      throw new CommandException("Invalid args: " + help());
    }
    int c = 1;
    String table = args[c++];
    boolean enqueue = Boolean.parseBoolean(args[c++]);
    int numberRows = Integer.parseInt(args[c++]);
    int numberRecordsPerRow = Integer.parseInt(args[c++]);
    int numberOfFamilies = Integer.parseInt(args[c++]);
    int numberOfColumns = Integer.parseInt(args[c++]);
    int numberOfWords = Integer.parseInt(args[c++]);
    int batch = Integer.parseInt(args[c++]);
    try {
      LoadData.runLoad(client, enqueue, table, numberRows, numberRecordsPerRow, numberOfFamilies, numberOfColumns,
          numberOfWords, batch, out);
    } catch (IOException e) {
      out.println("Error " + e.getMessage());
      if (Main.debug) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public String description() {
    return "Load test data.";
  }

  @Override
  public String usage() {
    return "<tablename> <queue true/false> <rows> <recordsPerRow> <families> <columnsPerRecord> <wordsPerColumn> <batchSize>";
  }

  @Override
  public String name() {
    return "loadtestdata";
  }
}
