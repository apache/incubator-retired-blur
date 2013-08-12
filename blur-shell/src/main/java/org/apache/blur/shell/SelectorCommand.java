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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Selector;

public class SelectorCommand extends Command {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length < 1) {
      throw new CommandException("Invalid args: " + help());
    }
    String command = null;
    if (args.length > 1) {
      command = args[1];
    }
    if (command == null) {
      List<String> columnFamiliesToFetch = Main.selector.getColumnFamiliesToFetch();
      Map<String, Set<String>> columnsToFetch = Main.selector.getColumnsToFetch();
      List<String> names = new ArrayList<String>();
      if (columnFamiliesToFetch != null) {
        names.addAll(columnFamiliesToFetch);
      } else {
        columnFamiliesToFetch = new ArrayList<String>();
      }
      if (columnsToFetch != null) {
        names.addAll(columnsToFetch.keySet());
      } else {
        columnsToFetch = new HashMap<String, Set<String>>();
      }
      for (String family : names) {
        if (columnFamiliesToFetch.contains(family)) {
          out.println(family + " - All Columns");
        } else {
          out.println(family + " - " + new TreeSet<String>(columnsToFetch.get(family)));
        }
      }
    } else if (command.equals("reset")) {
      Main.selector = new Selector();
    } else if (command.equals("add")) {
      if (args.length < 3) {
        throw new CommandException("Invalid args: " + help());
      }
      String family = args[2];
      if (args.length > 3) {
        if (Main.selector.columnsToFetch == null) {
          Main.selector.columnsToFetch = new HashMap<String, Set<String>>(); 
        }
        Set<String> columns = Main.selector.columnsToFetch.get(family);
        if (columns == null) {
          columns = new TreeSet<String>();
          Main.selector.columnsToFetch.put(family, columns);
        }
        for (int i = 3; i < args.length; i++) {
          columns.add(args[i]);
        }
        if (!columns.isEmpty()) {
          Main.selector.putToColumnsToFetch(family, columns);
        } else {
          Main.selector.addToColumnFamiliesToFetch(family);
        }
      } else {
        Main.selector.addToColumnFamiliesToFetch(family);
      }
    } else {
      throw new CommandException("Invalid args: " + help());
    }
  }

  @Override
  public String help() {
    return "manage the default selector, args; [reset] | [add family [columnName*]]";
  }
}
