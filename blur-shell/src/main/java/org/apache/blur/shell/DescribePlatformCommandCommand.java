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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.ArgumentDescriptor;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.CommandDescriptor;

public class DescribePlatformCommandCommand extends Command implements CommandFirstArgCommand {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length != 2) {
      throw new CommandException("Invalid args: " + help());
    }
    String commandName = args[1];

    List<CommandDescriptor> listInstalledCommands = client.listInstalledCommands();
    for (CommandDescriptor commandDescriptor : listInstalledCommands) {
      if (commandDescriptor.getCommandName().equals(commandName)) {
        print(out, commandDescriptor);
        return;
      }
    }
  }

  private void print(PrintWriter out, CommandDescriptor commandDescriptor) {
    String commandName = commandDescriptor.getCommandName();
    int width = 20;
    out.println(addWhiteSpace("Name:", width) + commandName);
    String description = commandDescriptor.getDescription();
    if (description != null) {
      out.println(addWhiteSpace("Description:", width) + description);
    }

    String returnType = commandDescriptor.getReturnType();
    out.println(addWhiteSpace("Return Type:", width) + returnType);
    String version = commandDescriptor.getVersion();
    if (version.equals("0")) {
      out.println(addWhiteSpace("Version:", width) + "Installed from classpath.");
    } else {
      out.println(addWhiteSpace("Version:", width) + version);
    }

    Map<String, ArgumentDescriptor> requiredArguments = commandDescriptor.getRequiredArguments();
    if (requiredArguments != null && !requiredArguments.isEmpty()) {
      out.println();
      out.println("Required Arguments:");
      for (Entry<String, ArgumentDescriptor> e : requiredArguments.entrySet()) {
        out.println(addWhiteSpace("-" + e.getKey(), width) + toString(e.getValue()));
      }
    }
    Map<String, ArgumentDescriptor> optionalArguments = commandDescriptor.getOptionalArguments();
    if (optionalArguments != null && !optionalArguments.isEmpty()) {
      out.println();
      out.println("Optional Arguments");
      for (Entry<String, ArgumentDescriptor> e : optionalArguments.entrySet()) {
        out.println(addWhiteSpace("-" + e.getKey(), width) + toString(e.getValue()));
      }
    }
    out.println();
  }

  private String toString(ArgumentDescriptor argumentDescriptor) {
    String type = argumentDescriptor.getType();
    String description = argumentDescriptor.getDescription();
    if (description == null) {
      return "Type: [" + type + "]";
    } else {
      return "Type: [" + type + "] Description: " + description;
    }
  }

  @Override
  public String description() {
    return "Describes the specifed command.";
  }

  @Override
  public String usage() {
    return "<command name>";
  }

  @Override
  public String name() {
    return "command-desc";
  }
}
