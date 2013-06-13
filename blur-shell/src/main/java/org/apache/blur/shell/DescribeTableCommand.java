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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.AlternateColumnDefinition;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.ColumnFamilyDefinition;
import org.apache.blur.thrift.generated.TableDescriptor;

public class DescribeTableCommand extends Command {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length != 2) {
      throw new CommandException("Invalid args: " + help());
    }
    String tablename = args[1];

    TableDescriptor describe = client.describe(tablename);
    out.println("cluster               : " + describe.cluster);
    out.println("name                  : " + describe.name);
    out.println("enabled               : " + describe.isEnabled);
    out.println("tableUri              : " + describe.tableUri);
    out.println("shardCount            : " + describe.shardCount);
    out.println("readOnly              : " + describe.readOnly);
    out.println("columnPreCache        : " + describe.columnPreCache);
    out.println(" - Other Options -");

    out.println("blockCaching          : " + describe.blockCaching);
    out.println("blockCachingFileTypes : " + describe.blockCachingFileTypes);
    out.println("tableProperties       : " + describe.tableProperties);

    out.println(" - Analyzer Definition -");
    print(0, out, describe.getAnalyzerDefinition());
  }

  private void print(int indent, PrintWriter out, AnalyzerDefinition analyzerDefinition) {
    ColumnDefinition defaultDefinition = analyzerDefinition.getDefaultDefinition();
    indent(out, indent).println("defaultDefinition: ");
    print(indent + 1, out, defaultDefinition);
    String fullTextAnalyzerClassName = analyzerDefinition.getFullTextAnalyzerClassName();
    indent(out, indent).println("fullTextAnalyzerClassName: " + fullTextAnalyzerClassName);
    Map<String, ColumnFamilyDefinition> columnFamilyDefinitions = analyzerDefinition.getColumnFamilyDefinitions();
    if (columnFamilyDefinitions != null) {
      indent(out, indent).println("columnFamilyDefinitions: ");
      for (Entry<String, ColumnFamilyDefinition> colFamDef : columnFamilyDefinitions.entrySet()) {
        String family = colFamDef.getKey();
        indent(out, indent + 1).println("family: " + family);
        ColumnFamilyDefinition value = colFamDef.getValue();
        print(indent + 2, out, value);
      }
    }
  }

  private PrintWriter indent(PrintWriter out, int indent) {
    for (int i = 0; i < indent; i++) {
      out.print("  ");
    }
    return out;
  }

  private void print(int indent, PrintWriter out, ColumnDefinition columnDefinition) {
    String analyzerClassName = columnDefinition.getAnalyzerClassName();
    indent(out, indent).println("analyzerClassName: " + analyzerClassName);
    Map<String, AlternateColumnDefinition> alternateColumnDefinitions = columnDefinition
        .getAlternateColumnDefinitions();
    if (alternateColumnDefinitions != null) {
      indent(out, indent).println("alternateColumnDefinitions: ");
      for (Entry<String, AlternateColumnDefinition> e : alternateColumnDefinitions.entrySet()) {
        indent(out, indent + 1).println("alternateDefName: " + e.getKey());
        AlternateColumnDefinition alternateColumnDefinition = e.getValue();
        indent(out, indent + 1).println("analyzerClassName: " + alternateColumnDefinition.getAnalyzerClassName());
      }
    }

  }

  private void print(int indent, PrintWriter out, ColumnFamilyDefinition value) {
    ColumnDefinition defaultDefinition = value.getDefaultDefinition();
    indent(out, indent).println("defaultDefinition: ");
    print(indent + 1, out, defaultDefinition);
    Map<String, ColumnDefinition> columnDefinitions = value.getColumnDefinitions();
    if (columnDefinitions != null) {
      indent(out, indent).println("columnDefinitions: ");
      for (Entry<String, ColumnDefinition> e : columnDefinitions.entrySet()) {
        String column = e.getKey();
        indent(out, indent + 1).println("column: " + column);
        ColumnDefinition columnDefinition = e.getValue();
        indent(out, indent + 1).println("columnDefinition: " + columnDefinition);
      }
    }
  }

  @Override
  public String help() {
    return "describe the named table, args; tablename";
  }
}
