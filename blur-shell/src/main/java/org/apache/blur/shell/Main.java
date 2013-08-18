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
import org.apache.blur.thrift.BadConnectionException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.BlurClientManager;
import org.apache.blur.thrift.Connection;
import org.apache.blur.thrift.commands.BlurCommand;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Selector;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class Main {
  static final String PROMPT = "blur> ";
  /** is debugging enabled - off by default */
  static boolean debug = false;
  /** is timing enabled - off by default */
  static boolean timed = false;
  /** is highlight enabled - off by default */
  static boolean highlight = false;
  /** default selector */
  static Selector selector = new Selector();

  private static Map<String, Command> commands;
  static String cluster;

  static String getCluster(Iface client) throws BlurException, TException, CommandException {
    return getCluster(client,
        "There is more than one shard cluster, use \"cluster\" command to set the cluster that should be in use.");
  }

  static String getCluster(Iface client, String errorMessage) throws BlurException, TException, CommandException {
    if (cluster != null) {
      return cluster;
    } else {
      List<String> shardClusterList = client.shardClusterList();
      if (shardClusterList.size() == 1) {
        cluster = shardClusterList.get(0);
        return cluster;
      }
      throw new CommandException(errorMessage);
    }
  }

  public static void usage() {
    System.out.println("Usage: java " + Main.class.getName()
        + " [controller1:port,controller2:port,...] [command] [options]");
  }

  private static class CliShellOptions {

    private String _controllerConnectionString;
    private boolean _shell;
    private String[] _args;
    private String _command;

    public boolean isShell() {
      return _shell;
    }

    public String getControllerConnectionString() {
      return _controllerConnectionString;
    }

    public String getCommand() {
      return _command;
    }

    public String[] getArgs() {
      return _args;
    }

    public void setControllerConnectionString(String controllerConnectionString) {
      this._controllerConnectionString = controllerConnectionString;
    }

    public void setShell(boolean shell) {
      this._shell = shell;
    }

    public void setArgs(String[] args) {
      this._args = args;
    }

    public void setCommand(String command) {
      this._command = command;
    }

  }

  private static class ClusterCommand extends Command {

    @Override
    public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
        BlurException {
      if (args.length != 2) {
        throw new CommandException("Invalid args: " + help());
      }
      String clusterNamePassed = args[1];
      if (validateClusterName(client, clusterNamePassed)) {
        cluster = clusterNamePassed;
        out.println("cluster is now " + cluster);
      } else {
        out.println("[ " + clusterNamePassed + " ]" + " is not a valid cluster name.");
      }
    }

    private boolean validateClusterName(Iface client, String clusterName) throws BlurException, TException {
      List<String> clusterNamesList = client.shardClusterList();
      if (clusterNamesList != null && !clusterNamesList.isEmpty()) {
        if (clusterNamesList.contains(clusterName)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String help() {
      return "set the cluster in use, args; clustername";
    }

  }

  private static class ResetCommand extends Command {

    @Override
    public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
        BlurException {
      ConsoleReader reader = getConsoleReader();
      if (reader != null) {
        try {
          reader.setPrompt("");
          reader.clearScreen();
        } catch (IOException e) {
          if (debug) {
            e.printStackTrace();
          }
          throw new CommandException(e.getMessage());
        }
      }
    }

    @Override
    public String help() {
      return "resets the terminal window";
    }

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

  private static class HighlightCommand extends Command {

    @Override
    public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
        BlurException {
      if (highlight == true) {
        highlight = false;
      } else {
        highlight = true;
      }
      out.println("highlight of query command is now " + (highlight ? "on" : "off"));
    }

    @Override
    public String help() {
      return "toggle highlight of query output on/off";
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
      String[] tableCommands = { "create", "enable", "disable", "remove", "truncate", "describe", "list", "schema",
          "stats", "layout", "parse" };
      printCommandAndHelp(out, cmds, tableCommands, bufferLength);

      out.println();
      out.println(" - Data commands - ");
      String[] dataCommands = { "query", "get", "mutate", "delete", "highlight", "selector" };
      printCommandAndHelp(out, cmds, dataCommands, bufferLength);

      out.println();
      out.println(" - Cluster commands - ");
      String[] clusterCommands = { "controllers", "shards", "clusterlist", "cluster", "safemodewait", "top" };
      printCommandAndHelp(out, cmds, clusterCommands, bufferLength);

      out.println();
      out.println(" - Shell commands - ");
      String[] shellCommands = { "help", "debug", "timed", "quit", "reset" };
      printCommandAndHelp(out, cmds, shellCommands, bufferLength);

      if (!cmds.isEmpty()) {
        out.println();
        out.println(" - Other operations - ");

        for (Entry<String, Command> e : cmds.entrySet()) {
          out.println("  " + buffer(e.getKey(), bufferLength) + " - " + e.getValue().help());
        }
      }

      out.println();
      out.println("  " + buffer("shell", bufferLength) + " - enters into the Blur interactive shell");
      out.println("  " + buffer("execute", bufferLength)
          + " - executes a custom class passing all the command line args to the main method");
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

    args = removeLeadingShellFromScript(args);

    Builder<String, Command> builder = new ImmutableMap.Builder<String, Command>();
    builder.put("help", new HelpCommand());
    builder.put("debug", new DebugCommand());
    builder.put("timed", new TimedCommand());
    builder.put("highlight", new HighlightCommand());
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
    builder.put("truncate", new TruncateTableCommand());
    builder.put("cluster", new ClusterCommand());
    builder.put("safemodewait", new WaitInSafemodeCommand());
    builder.put("top", new TopCommand());
    builder.put("parse", new ParseCommand());
    builder.put("loadtestdata", new LoadTestDataCommand());
    builder.put("selector", new SelectorCommand());
    builder.put("definecolumn", new AddColumnDefinitionCommand());
    builder.put("reset", new ResetCommand());
    commands = builder.build();

    CliShellOptions cliShellOptions = getCliShellOptions(args);
    if (cliShellOptions == null) {
      return;
    }

    try {
      Blur.Iface client = BlurClient.getClient(cliShellOptions.getControllerConnectionString());
      if (cliShellOptions.isShell()) {
        ConsoleReader reader = new ConsoleReader();
        PrintWriter out = new PrintWriter(reader.getOutput());
        setConsoleReader(commands, reader);
        setPrompt(client, reader, cliShellOptions.getControllerConnectionString(), out);

        List<Completer> completors = new LinkedList<Completer>();

        completors.add(new StringsCompleter(commands.keySet()));
        completors.add(new FileNameCompleter());

        for (Completer c : completors) {
          reader.addCompleter(c);
        }

        String line;
        try {
          while ((line = reader.readLine()) != null) {
            line = line.trim();
            // ignore empty lines and comments
            if (line.length() == 0 || line.startsWith("#")) {
              continue;
            }
            String[] commandArgs = line.split("\\s+");
            String commandStr = commandArgs[0];
            if (commandStr.equals("exit")) {
              commandStr = "quit";
            }
            Command command = commands.get(commandStr);
            if (command == null) {
              out.println("unknown command \"" + commandStr + "\"");
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
              } catch (BadConnectionException e) {
                out.println(e.getMessage());
                if (debug) {
                  e.printStackTrace(out);
                }
              } finally {
                if (timed) {
                  out.println("Last command took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
                }
              }
              setPrompt(client, reader, cliShellOptions.getControllerConnectionString(), out);
            }
          }
        } finally {
          out.close();
        }
      } else {
        Command command = commands.get(cliShellOptions.getCommand());
        PrintWriter out = new PrintWriter(System.out);
        try {
          command.doit(out, client, cliShellOptions.getArgs());
        } catch (CommandException e) {
          out.println(e.getMessage());
        } catch (BlurException e) {
          out.println(e.getMessage());
          e.printStackTrace(out);
        } catch (BadConnectionException e) {
          out.println(e.getMessage());
        }
        out.close();
        return;
      }
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }

  private static void setPrompt(Iface client, ConsoleReader reader, String connectionStr, PrintWriter out)
      throws BlurException, TException, CommandException, IOException {
    List<String> shardClusterList;
    try {
      shardClusterList = BlurClientManager.execute(connectionStr, new BlurCommand<List<String>>() {
        @Override
        public List<String> call(Client client) throws BlurException, TException {
          return client.shardClusterList();
        }
      }, 0, 0, 0);
    } catch (BadConnectionException e) {
      out.println(e.getMessage() + " Connection (" + connectionStr + ")");
      out.flush();
      if (debug) {
        e.printStackTrace(out);
      }
      System.exit(1);
      throw e; // will never be called
    }
    String currentPrompt = reader.getPrompt();
    String prompt;
    if (shardClusterList.size() == 1) {
      prompt = "blur (" + getCluster(client) + ")> ";
    } else if (cluster == null) {
      prompt = PROMPT;
    } else {
      prompt = "blur (" + cluster + ")> ";
    }
    if (currentPrompt == null || !currentPrompt.equals(prompt)) {
      reader.setPrompt(prompt);
    }
  }

  private static String[] removeLeadingShellFromScript(String[] args) {
    if (args.length > 0) {
      if (args[0].equals("shell")) {
        String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        return newArgs;
      }
    }
    return args;
  }

  private static CliShellOptions getCliShellOptions(String[] args) throws IOException {
    CliShellOptions cliShellOptions = new CliShellOptions();
    if (args.length == 0) {
      String controllerConnectionString = loadControllerConnectionString();
      if (controllerConnectionString == null) {
        System.err
            .println("Could not locate controller connection string in the blu-site.properties file and it was not passed in via command line args.");
        return null;
      }
      cliShellOptions.setControllerConnectionString(controllerConnectionString);
      cliShellOptions.setShell(true);
      return cliShellOptions;
    } else {
      String arg0 = args[0];
      Command command = commands.get(arg0);
      if (command == null) {
        // then might be controller string
        try {
          new Connection(arg0);
        } catch (RuntimeException e) {
          String message = e.getMessage();
          System.err.println("Arg [" + arg0 + "] could not be located as a command and is not a connection string. ["
              + message + "]");
          return null;
        }
        cliShellOptions.setControllerConnectionString(arg0);
        if (args.length > 1) {
          // there's might be a command after the connection string
          cliShellOptions.setShell(false);
          String arg1 = args[1];
          command = commands.get(arg1);
          if (command == null) {
            System.err.println("Command [" + arg1 + "] not found");
            return null;
          } else {
            cliShellOptions.setCommand(arg1);
            String[] newArgs = new String[args.length - 1];
            System.arraycopy(args, 1, newArgs, 0, newArgs.length);
            cliShellOptions.setArgs(newArgs);
            return cliShellOptions;
          }
        } else {
          cliShellOptions.setShell(true);
          return cliShellOptions;
        }
      } else {
        String controllerConnectionString = loadControllerConnectionString();
        if (controllerConnectionString == null) {
          System.err
              .println("Could not locate controller connection string in the blur-site.properties file and it was not passed in via command line args.");
          return null;
        }
        cliShellOptions.setControllerConnectionString(controllerConnectionString);
        // command was found at arg0
        cliShellOptions.setShell(false);
        cliShellOptions.setArgs(args);
        cliShellOptions.setCommand(arg0);
        return cliShellOptions;
      }
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
