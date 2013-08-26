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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public class CreateTableCommand extends Command {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    CommandLine cmd = CreateTableCommandHelper.parse(args, out);
    if (cmd == null) {
      throw new CommandException(name() + " missing required arguments");
    }
    TableDescriptor td = new TableDescriptor();

    td.setCluster(Main.getCluster(client));
    td.setName(cmd.getOptionValue("t"));
    td.setShardCount(Integer.parseInt(cmd.getOptionValue("c")));

    if (cmd.hasOption("b")) {
      td.setBlockCaching(false);
    }
    if (cmd.hasOption("B")) {
      String[] optionValues = cmd.getOptionValues("B");
      Set<String> blockCachingFileTypes = new HashSet<String>();
      if (optionValues != null) {
        blockCachingFileTypes.addAll(Arrays.asList(cmd.getOptionValues("B")));
      }
      td.setBlockCachingFileTypes(blockCachingFileTypes);
    }
    if (cmd.hasOption("mfi")) {
      td.setDefaultMissingFieldLessIndexing(false);
    }
    if (cmd.hasOption("mft")) {
      String defaultMissingFieldType = cmd.getOptionValue("mft");
      td.setDefaultMissingFieldType(defaultMissingFieldType);
    }
    if (cmd.hasOption("mfp")) {
      Map<String, String> defaultMissingFieldProps = getProps(cmd, "mfp");
      td.setDefaultMissingFieldProps(defaultMissingFieldProps);
    }
    if (cmd.hasOption("d")) {
      td.setEnabled(false);
    }
    if (cmd.hasOption("p")) {
      Map<String, String> tableProperties = getProps(cmd, "p");
      td.setTableProperties(tableProperties);
    }
    if (cmd.hasOption("s")) {
      td.setStrictTypes(true);
    }
    if (cmd.hasOption("r")) {
      td.setReadOnly(true);
    }
    if (cmd.hasOption("l")) {
      String tableUri = cmd.getOptionValue("l");
      td.setReadOnly(true);
      td.setTableUri(tableUri);
    }
    if (cmd.hasOption("P")) {
      td.setPreCacheCols(Arrays.asList(cmd.getOptionValues("P")));
    }
    if (cmd.hasOption("S")) {
      td.setSimilarityClass(cmd.getOptionValue("S"));
    }

    if (Main.debug) {
      out.println(td.toString());
      out.flush();
    }

    client.createTable(td);
  }

  private Map<String, String> getProps(CommandLine cmd, String opt) {
    Map<String, String> props = new HashMap<String, String>();
    Option[] options = cmd.getOptions();
    for (Option option : options) {
      if (option.getOpt().equals(opt)) {
        String[] values = option.getValues();
        props.put(values[0], values[1]);
      }
    }
    return props;
  }

  @Override
  public String description() {
    return "Create the named table.  Run -h for full argument list.";
  }

  @Override
  public String usage() {
    return "-t <tablename> -c <shardcount>";
  }

  @Override
  public String name() {
    return "create";
  }

}
