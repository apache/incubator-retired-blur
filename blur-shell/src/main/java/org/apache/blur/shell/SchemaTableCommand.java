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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Schema;

public class SchemaTableCommand extends Command implements TableFirstArgCommand {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length < 2) {
      throw new CommandException("Invalid args: " + help());
    }
    String tablename = args[1];
    List<String> familiesToDisplay = new ArrayList<String>();
    for (int i = 2; i < args.length; i++) {
      familiesToDisplay.add(args[i]);
    }

    Schema schema = client.schema(tablename);
    out.println("table  : "+schema.getTable());
    Map<String, Map<String, ColumnDefinition>> families = schema.getFamilies();
    Set<String> familyNames = new TreeSet<String>(families.keySet());
    for (String cf : familyNames) {
      if (!familiesToDisplay.isEmpty() && !familiesToDisplay.contains(cf)) {
        continue;
      }
      out.println("family : " + cf);
      Map<String, ColumnDefinition> columns = families.get(cf);
      Set<String> columnNames = new TreeSet<String>(columns.keySet());
      for (String c : columnNames) {
        ColumnDefinition columnDefinition = columns.get(c);
        out.println("\tcolumn   : " + columnDefinition.getColumnName());
        String fieldType = columnDefinition.getFieldType();
        Map<String, String> properties = columnDefinition.getProperties();
        String subColumnName = columnDefinition.getSubColumnName();
        if (subColumnName != null) {
          out.println("\t\t\tsubName   : " + subColumnName);
          out.println("\t\t\tfieldType : " + fieldType);
          if (properties != null) {
            Map<String, String> props = new TreeMap<String, String>(properties);
            for (Entry<String, String> e : props.entrySet()) {
              out.println("\t\t\tprop      : " + e);
            }
          }
        } else {
          out.println("\t\tfieldType : " + fieldType);
          if (properties != null) {
            Map<String, String> props = new TreeMap<String, String>(properties);
            for (Entry<String, String> e : props.entrySet()) {
              out.println("\t\tprop      : " + e);
            }
          }
        }
      }
    }
    for (String f : familiesToDisplay) {
      if (!familyNames.contains(f)) {
        out.println("family : " + f + " NOT FOUND");
      }
    }
  }

  @Override
  public String description() {
    return "Schema of the named table.";
  }

  @Override
  public String usage() {
    return "<tablename> [<family> ...]";
  }

  @Override
  public String name() {
    return "schema";
  }
}
