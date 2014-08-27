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
import java.io.Writer;
import java.util.List;
import java.util.Map;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class CopyTableCommand extends Command {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    CommandLine cmd = CopyTableCommand.parse(args, out);
    if (cmd == null) {
      throw new CommandException(name() + " missing required arguments");
    }
    
    String src = cmd.getOptionValue("src");
    String dest = cmd.getOptionValue("dest");
    String location = cmd.getOptionValue("l");
    String cluster = cmd.getOptionValue("c");
    
    List<String> tables = client.tableList();
    
    if(!tables.contains(src)) {
      throw new IllegalArgumentException("Unable to find source table[" + src + "]");
    }
    
    List<String> clusters = client.shardClusterList();
    
    if(!clusters.contains(cluster)) {
      throw new IllegalArgumentException("Unknown destination cluster[" + cluster +"]");
    }
    
    List<String> targetTables = client.tableListByCluster(cluster);
    
    if(targetTables.contains(dest)) {
      throw new IllegalArgumentException("Target cluster[" + cluster + "] already contains target table[" + dest + "]");
    }
    
    TableDescriptor td = client.describe(src);
    
    td.setTableUri(location);
    td.setCluster(cluster);
    td.setName(dest);
    
    if(Main.debug) {
      out.println(td.toString());
      out.flush();
    }
    client.createTable(td);
    
    Schema schema = client.schema(src);
    
    for(Map<String, ColumnDefinition> column : schema.getFamilies().values()) {
    	for (ColumnDefinition def : column.values()) {
    		client.addColumnDefinition(dest, def);
    	}
    }
  }
    
  @Override
  public String description() {
    return "Copy the table definitions to a new table.  Run -h for full argument list.";
  }

  @Override
  public String usage() {
    return "-src <tablename> -dest <desttable> -l <location> -c <cluster>";
  }

  @Override
  public String name() {
    return "copy";
  }
  @SuppressWarnings("static-access")
  public static CommandLine parse(String[] otherArgs, Writer out) {
    Options options = new Options();

    options.addOption(
        OptionBuilder
        .isRequired()
        .hasArg()
        .withArgName("tablename")
        .withDescription("* Source table name.")
        .create("src"));
    
    options.addOption(
        OptionBuilder
        .isRequired()
        .hasArg()
        .withArgName("tablename")
        .withDescription("* Target table name.")
        .create("dest"));
    
    options.addOption(
        OptionBuilder
        .isRequired()
        .hasArg()
        .withArgName("cluster")
        .withDescription("* Target cluster for new table.")
        .create("c"));
    
    options.addOption(
        OptionBuilder
        .hasArg()
        .isRequired()
        .withArgName("uri")
        .withDescription("The location of the target table. (Example hdfs://namenode/blur/tables/table)")
        .create("l"));
    
    options.addOption(
        OptionBuilder
        .withDescription("Displays help for this command.")
        .create("h"));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, otherArgs);
      if (cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(out, true);
        formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "copy", null, options,
            HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null, false);
        return null;
      }
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(out, true);
      formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "copy", null, options,
          HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }
    return cmd;
  }
}
