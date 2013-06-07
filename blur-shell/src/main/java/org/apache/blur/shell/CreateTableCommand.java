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

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;

public class CreateTableCommand extends Command {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args)
      throws CommandException, TException, BlurException {
    if (args.length != 4) {
      throw new CommandException("Invalid args: " + help());
    }
    String tableuri = args[1];
    String tablename = args[2];
    int shardCount = Integer.parseInt(args[3]);

    AnalyzerDefinition ad = new AnalyzerDefinition();

    TableDescriptor td = new TableDescriptor(); 
    td.setTableUri(tableuri);
    td.setCluster("default");
    td.setAnalyzerDefinition(ad);
    td.setName(tablename);

    td.setShardCount(shardCount);

    if (Main.debug) {
      out.println(td.toString());
      out.flush();
    }

    client.createTable(td);
  }

  @Override
  public String help() {
    return "create the named table, args; tableuri tablename shardcount";
  }
}
