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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;

import org.apache.blur.shell.Command.CommandException;
import org.apache.blur.shell.Main.QuitCommand.QuitCommandException;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class Main {
  /** is debugging enabled - off by default */
  static boolean debug = false;
  /** is timing enabled - off by default */
  static boolean timed = false;

  private static Map<String, Command> commands;

  public static void usage() {
    System.out.println("Usage: java " + Main.class.getName() + " controller1:port,controller2:port,...");
  }

  private static class DebugCommand extends Command {

    @Override
    public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
        BlurException {
      if (debug == true) {
        debug = false;
      } else {
        debug = true;
      }
      out.println("debugging is now " + (debug ? "on" : "off"));
    }

    @Override
    public String help() {
      return "toggle debugging on/off";
    }

  }

  private static class TimedCommand extends Command {

    @Override
    public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
        BlurException {
      if (timed == true) {
        timed = false;
      } else {
        timed = true;
      }
      out.println("timing of commands is now " + (timed ? "on" : "off"));
    }

    @Override
    public String help() {
      return "toggle timing of commands on/off";
    }

  }

  private static class HelpCommand extends Command {
    @Override
    public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
        BlurException {
      out.println("Available commands:");

      Map<String, Command> cmds = new TreeMap<String, Command>(commands);

      int bufferLength = getMaxCommandLength(cmds.keySet()) + 2;
      out.println(" - Table commands - ");
      String[] tableCommands = { "create", "enable", "disable", "remove", "describe", "list", "schema", "stats",
          "layout" };
      printCommandAndHelp(out, cmds, tableCommands, bufferLength);

      out.println();
      out.println(" - Data commands - ");
      String[] dataCommands = { "query", "get", "mutate", "delete" };
      printCommandAndHelp(out, cmds, dataCommands, bufferLength);

      out.println();
      out.println(" - Cluster commands - ");
      String[] clusterCommands = { "controllers", "shards", "clusterlist" };
      printCommandAndHelp(out, cmds, clusterCommands, bufferLength);

      out.println();
      out.println(" - Shell commands - ");
      String[] shellCommands = { "help", "debug", "timed", "quit" };
      printCommandAndHelp(out, cmds, shellCommands, bufferLength);

      if (!cmds.isEmpty()) {
        out.println();
        out.println(" - Other operations - ");

        for (Entry<String, Command> e : cmds.entrySet()) {
          out.println("  " + buffer(e.getKey(), bufferLength) + " - " + e.getValue().help());
        }
      }
    }

    private int getMaxCommandLength(Set<String> keySet) {
      int result = 0;
      for (String s : keySet) {
        if (s.length() > result) {
          result = s.length();
        }
      }
      return result;
    }

    private void printCommandAndHelp(PrintWriter out, Map<String, Command> cmds, String[] tableCommands,
        int bufferLength) {
      for (String c : tableCommands) {
        Command command = cmds.remove(c);
        out.println("  " + buffer(c, bufferLength) + " - " + command.help());
      }
    }

    private String buffer(String s, int bufferLength) {
      while (s.length() < bufferLength) {
        s = s + " ";
      }
      return s;
    }

    @Override
    public String help() {
      return "display help";
    }
  }

  public static class QuitCommand extends Command {
    @SuppressWarnings("serial")
    public static class QuitCommandException extends CommandException {
      public QuitCommandException() {
        super("quit");
      }
    }

    @Override
    public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
        BlurException {
      throw new QuitCommandException();
    }

    @Override
    public String help() {
      return "exit the shell";
    }
  }

  public static void main(String[] args) throws Throwable {
    Builder<String, Command> builder = new ImmutableMap.Builder<String, Command>();
    builder.put("help", new HelpCommand());
    builder.put("debug", new DebugCommand());
    builder.put("timed", new TimedCommand());
    builder.put("quit", new QuitCommand());
    builder.put("list", new ListTablesCommand());
    builder.put("create", new CreateTableCommand());
    builder.put("enable", new EnableDisableTableCommand());
    builder.put("disable", new EnableDisableTableCommand());
    builder.put("remove", new RemoveTableCommand());
    builder.put("describe", new DescribeTableCommand());
    builder.put("stats", new TableStatsCommand());
    builder.put("schema", new SchemaTableCommand());
    builder.put("query", new QueryCommand());
    builder.put("get", new GetRowCommand());
    builder.put("delete", new DeleteRowCommand());
    builder.put("mutate", new MutateRowCommand());
    builder.put("indexaccesslog", new IndexAccessLogCommand());
    builder.put("clusterlist", new ShardClusterListCommand());
    builder.put("layout", new ShardServerLayoutCommand());
    builder.put("controllers", new ControllersEchoCommand());
    builder.put("shards", new ShardsEchoCommand());
    commands = builder.build();

    try {
      ConsoleReader reader = new ConsoleReader();
      
      setConsoleReader(commands, reader);
      
      reader.setPrompt("blur> ");

      String controllerConnectionString = null;

      if ((args == null) || (args.length != 1)) {
        controllerConnectionString = loadControllerConnectionString();
        if (controllerConnectionString == null) {
          usage();
          return;
        }
      } else {
        controllerConnectionString = args[0];

      }

      List<Completer> completors = new LinkedList<Completer>();

      completors.add(new StringsCompleter(commands.keySet()));
      completors.add(new FileNameCompleter());

      for (Completer c : completors) {
        reader.addCompleter(c);
      }

      Blur.Iface client = BlurClient.getClient(controllerConnectionString);

      String line;
      PrintWriter out = new PrintWriter(reader.getOutput());
      try {
        while ((line = reader.readLine()) != null) {
          line = line.trim();
          // ignore empty lines and comments
          if (line.length() == 0 || line.startsWith("#")) {
            continue;
          }
          String[] commandArgs = line.split("\\s");
          Command command = commands.get(commandArgs[0]);
          if (command == null) {
            out.println("unknown command \"" + commandArgs[0] + "\"");
          } else {
            long start = System.nanoTime();
            try {
              command.doit(out, client, commandArgs);
            } catch (QuitCommandException e) {
              // exit gracefully
              System.exit(0);
            } catch (CommandException e) {
              out.println(e.getMessage());
              if (debug) {
                e.printStackTrace(out);
              }
            } catch (BlurException e) {
              out.println(e.getMessage());
              if (debug) {
                e.printStackTrace(out);
              }
            } finally {
              if (timed) {
                out.println("Last command took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
              }
            }
          }
        }
      } finally {
        out.close();
      }

    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }

  private static void setConsoleReader(Map<String, Command> cmds, ConsoleReader reader) {
    for (Entry<String, Command> c : cmds.entrySet()) {
      c.getValue().setConsoleReader(reader);
    }
  }

  private static String loadControllerConnectionString() throws IOException {
    StringBuilder builder = new StringBuilder();
    InputStream inputStream = Main.class.getResourceAsStream("/controllers");
    if (inputStream == null) {
      return null;
    }
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      if (builder.length() != 0) {
        builder.append(',');
      }
      String trim = line.trim();
      if (trim.startsWith("#")) {
        continue;
      }
      builder.append(trim).append(":40010");
    }
    bufferedReader.close();
    return builder.toString();
  }
}
