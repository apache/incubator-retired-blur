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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.blur.command.BlurObject;
import org.apache.blur.command.BlurObjectSerDe;
import org.apache.blur.command.CommandRunner;
import org.apache.blur.command.CommandUtil;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.Connection;
import org.apache.blur.thrift.generated.ArgumentDescriptor;
import org.apache.blur.thrift.generated.Arguments;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.CommandDescriptor;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class ExecutePlatformCommandCommand extends Command implements CommandFirstArgCommand {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length < 2) {
      throw new CommandException("Invalid args: " + help());
    }
    String commandName = args[1];

    CommandDescriptor command = getCommandDescriptor(client, commandName);
    if (command == null) {
      throw new CommandException("Command [" + commandName + "] not found.");
    }

    BlurObjectSerDe serde = new BlurObjectSerDe();
    BlurObject blurObject = getArgs(args, command, out);
    if (blurObject == null) {
      return;
    }
    Arguments arguments = CommandUtil.toArguments(blurObject);

    Connection[] connection = CommandRunner.getConnection(client);
    Object thriftObject;
    try {
      thriftObject = CommandRunner.runInternalReturnThriftObject(commandName, arguments, connection);
    } catch (IOException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
      throw new CommandException(e.getMessage());
    }
    Object javaObject = serde.fromSupportedThriftObject(thriftObject);
    out.println(javaObject);
  }

  private BlurObject getArgs(String[] args, CommandDescriptor command, PrintWriter out) throws CommandException {
    Options options = new Options();
    Map<String, ArgumentDescriptor> requiredArguments = new TreeMap<String, ArgumentDescriptor>(
        command.getRequiredArguments());
    addOptions(true, options, requiredArguments.entrySet());
    Map<String, ArgumentDescriptor> optionalArguments = new TreeMap<String, ArgumentDescriptor>(
        command.getOptionalArguments());
    addOptions(false, options, optionalArguments.entrySet());
    CommandLine commandLine = parse(command.getCommandName(), options, args, out);
    return createBlurObject(commandLine, command);
  }

  private BlurObject createBlurObject(CommandLine commandLine, CommandDescriptor descriptor) throws CommandException {
    Map<String, ArgumentDescriptor> arguments = new TreeMap<String, ArgumentDescriptor>(
        descriptor.getOptionalArguments());
    arguments.putAll(descriptor.getRequiredArguments());
    BlurObject blurObject = new BlurObject();
    if (commandLine == null) {
      return null;
    }
    Option[] options = commandLine.getOptions();
    for (Option option : options) {
      String name = option.getOpt();
      String value = option.getValue();
      ArgumentDescriptor argumentDescriptor = arguments.get(name);
      String type = argumentDescriptor.getType();
      blurObject.put(name, convertIfNeeded(value, type));
    }
    return blurObject;
  }

  private Object convertIfNeeded(String value, String type) throws CommandException {
    if (type.equals("String")) {
      return value;
    } else if (type.equals("Long")) {
      return Long.parseLong(value);
    } else if (type.equals("Integer")) {
      return Integer.parseInt(value);
    } else if (type.equals("Short")) {
      return Short.parseShort(value);
    } else if (type.equals("Float")) {
      return Float.parseFloat(value);
    } else if (type.equals("Double")) {
      return Double.parseDouble(value);
    } else if (type.equals("Boolean")) {
      return Boolean.parseBoolean(value);
    }
    throw new CommandException("Type of [" + type + "] is not supported by the shell.");
  }

  private CommandLine parse(String commandName, Options options, String[] args, PrintWriter out) {
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(out, true);
        formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, commandName, null, options,
            HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null, false);
        return null;
      }
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(out, true);
      formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, commandName, null, options, HelpFormatter.DEFAULT_LEFT_PAD,
          HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }
    return cmd;
  }

  private void addOptions(boolean required, Options options, Set<Entry<String, ArgumentDescriptor>> entrySet) {
    for (Entry<String, ArgumentDescriptor> e : entrySet) {
      String argumentName = e.getKey();
      ArgumentDescriptor argumentDescriptor = e.getValue();
      Option option = OptionBuilder.create(argumentName);
      option.setRequired(required);
      String description = argumentDescriptor.getDescription();
      option.setDescription(createDescription(description, required));
      option.setArgs(1);
      options.addOption(option);
    }
  }

  private String createDescription(String description, boolean required) {
    if (description == null) {
      description = "No description.";
    }
    if (required) {
      return "Required - " + description;
    } else {
      return "Optional - " + description;
    }
  }

  private CommandDescriptor getCommandDescriptor(Iface client, String commandName) throws BlurException, TException {
    List<CommandDescriptor> listInstalledCommands = client.listInstalledCommands();
    for (CommandDescriptor commandDescriptor : listInstalledCommands) {
      if (commandDescriptor.getCommandName().equals(commandName)) {
        return commandDescriptor;
      }
    }
    return null;
  }

  @Override
  public String description() {
    return "Execute a platform command.  Run -h for full argument list.";
  }

  @Override
  public String usage() {
    return "<command name> ...";
  }

  @Override
  public String name() {
    return "command-exec";
  }
}
