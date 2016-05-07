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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.shell.Command.CommandException;
import org.apache.blur.shell.Main.QuitCommand.QuitCommandException;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BadConnectionException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.Connection;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.trace.LogTraceStorage;
import org.apache.blur.trace.Trace;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class Main {
  static final String PROMPT = "blur> ";
  /** is debugging enabled - off by default */
  static boolean debug = false;
  /** is timing enabled - off by default */
  static boolean timed = false;
  /** is tracing enabled - off by default */
  static boolean trace = false;
  /** is highlight enabled - off by default */
  static boolean highlight = false;
  /** default selector */
  static Selector selector = new Selector();

  static Map<String, Command> commands;
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

    private Iface _client;
    private boolean _shell;
    private String[] _args;
    private String _command;

    public boolean isShell() {
      return _shell;
    }

    public Iface getClient() {
      return _client;
    }

    public String getCommand() {
      return _command;
    }

    public String[] getArgs() {
      return _args;
    }

    public void setClient(Iface client) {
      this._client = client;
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
    public String description() {
      return "Set the cluster in use.";
    }

    @Override
    public String usage() {
      return "<clustername>";
    }

    @Override
    public String name() {
      return "cluster";
    }

  }

  private static class WhoAmICommand extends Command {
    @Override
    public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
        BlurException {
      out.println("User [" + UserContext.getUser() + "]");
    }

    @Override
    public String description() {
      return "Print current user.";
    }

    @Override
    public String usage() {
      return "";
    }

    @Override
    public String name() {
      return "whoami";
    }
  }

  private static class UserCommand extends Command {

    @Override
    public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
        BlurException {
      if (args.length == 1) {
        UserContext.reset();
        out.println("User reset.");
        return;
      }
      String username = args[1];
      Map<String, String> attributes = new HashMap<String, String>();
      for (int i = 2; i < args.length; i++) {
        String[] parts = args[i].split("\\=");
        attributes.put(parts[0], parts[1]);
      }
      UserContext.setUser(new User(username, attributes));
      out.println("User set [" + UserContext.getUser() + "]");
    }

    @Override
    public String description() {
      return "Set the user in use. No args to reset.";
    }

    @Override
    public String usage() {
      return "[<username> [name=value ...]]";
    }

    @Override
    public String name() {
      return "user";
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
    public String description() {
      return "Resets the terminal window.";
    }

    @Override
    public String usage() {
      return "";
    }

    @Override
    public String name() {
      return "reset";
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
    public String description() {
      return "Toggle debugging on/off.";
    }

    @Override
    public String usage() {
      return "";
    }

    @Override
    public String name() {
      return "debug";
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
    public String description() {
      return "Toggle timing of commands on/off.";
    }

    @Override
    public String usage() {
      return "";
    }

    @Override
    public String name() {
      return "timed";
    }

  }

  private static class TraceCommand extends Command {

    @Override
    public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
        BlurException {
      if (trace == true) {
        trace = false;
      } else {
        trace = true;
      }
      out.println("tracing of commands is now " + (trace ? "on" : "off"));
    }

    @Override
    public String description() {
      return "Toggle tracing of commands on/off.";
    }

    @Override
    public String usage() {
      return "";
    }

    @Override
    public String name() {
      return "trace";
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
    public String description() {
      return "Toggle highlight of query output on/off.";
    }

    @Override
    public String usage() {
      return "";
    }

    @Override
    public String name() {
      return "highlight";
    }

  }

  public static String[] tableCommands = { "create", "enable", "disable", "remove", "truncate", "describe", "list",
      "schema", "stats", "layout", "parse", "definecolumn", "optimize", "copy" };
  public static String[] dataCommands = { "query", "get", "mutate", "delete", "highlight", "selector", "terms",
      "create-snapshot", "remove-snapshot", "list-snapshots", "import" };
  public static String[] clusterCommands = { "controllers", "shards", "clusterlist", "cluster", "safemodewait", "top" };
  public static String[] shellCommands = { "help", "debug", "timed", "quit", "reset", "user", "whoami", "trace",
      "trace-remove", "trace-list" };
  public static String[] platformCommands = { "command-list", "command-exec", "command-desc", "command-running",
      "command-cancel", "command-watch" };
  public static String[] serverCommands = { "logger", "logger-reset", "remove-shard" };

  private static class HelpCommand extends Command {
    @Override
    public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
        BlurException {

      Map<String, Command> cmds = new TreeMap<String, Command>(commands);
      if (args.length == 2) {
        String commandStr = args[1];
        out.println(" - " + commandStr + " help -");
        out.println();
        Command command = cmds.get(commandStr);
        if (command == null) {
          out.println("Command " + commandStr + " not found.");
          return;
        }
        out.println(command.helpWithDescription());
        out.println();
        return;
      }

      out.println("Available commands:");

      int bufferLength = getMaxCommandLength(cmds.keySet()) + 2;
      out.println(" - Table commands - ");
      printCommandAndHelp(out, cmds, tableCommands, bufferLength);

      out.println();
      out.println(" - Data commands - ");
      printCommandAndHelp(out, cmds, dataCommands, bufferLength);

      out.println();
      out.println(" - Cluster commands - ");
      printCommandAndHelp(out, cmds, clusterCommands, bufferLength);

      out.println();
      out.println(" - Server commands - ");
      printCommandAndHelp(out, cmds, serverCommands, bufferLength);

      out.println();
      out.println(" - Platform commands - ");
      printCommandAndHelp(out, cmds, platformCommands, bufferLength);

      out.println();
      out.println(" - Shell commands - ");

      printCommandAndHelp(out, cmds, shellCommands, bufferLength);

      if (!cmds.isEmpty()) {
        out.println();
        out.println(" - Other operations - ");
        for (Entry<String, Command> e : cmds.entrySet()) {
          out.println("  " + buffer(e.getKey(), bufferLength) + " - " + e.getValue().help());
        }
      }

      out.println();
      out.println("  " + buffer("shell", bufferLength) + " - enters into the Blur interactive shell.");
      out.println("  " + buffer("execute", bufferLength)
          + " - executes a custom class passing all the command line args to the main method.");
      out.println("  " + buffer("csvloader", bufferLength) + " - runs a MapReduce job to bulk load data into a table.");
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
    public String description() {
      return "Display help.";
    }

    @Override
    public String usage() {
      return "";
    }

    @Override
    public String name() {
      return "help";
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
    public String description() {
      return "Exit the shell.";
    }

    @Override
    public String usage() {
      return "";
    }

    @Override
    public String name() {
      return "quit";
    }
  }

  public static void main(String[] args) throws Throwable {

    Trace.setStorage(new LogTraceStorage(new BlurConfiguration()));

    args = removeLeadingShellFromScript(args);

    setupCommands();

    CliShellOptions cliShellOptions = getCliShellOptions(args);
    if (cliShellOptions == null) {
      return;
    }

    try {
      Blur.Iface client = cliShellOptions.getClient();
      if (cliShellOptions.isShell()) {
        ConsoleReader reader = new ConsoleReader();
        PrintWriter out = new PrintWriter(reader.getOutput());
        setConsoleReader(commands, reader);
        setPrompt(client, reader, out);

        List<Completer> completors = new LinkedList<Completer>();

        // completors.add(new StringsCompleter(commands.keySet()));
        // completors.add(new FileNameCompleter());
        completors.add(new CommandCompletor(commands, client));

        for (Completer c : completors) {
          reader.addCompleter(c);
        }

        reader.setCompletionHandler(new ShowDiffsOnlyCompletionHandler());

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
                String traceId = null;
                if (trace) {
                  traceId = UUID.randomUUID().toString();
                  Trace.setupTrace(traceId);
                  out.println("Running trace with id: " + traceId);
                }
                command.doit(out, client, commandArgs);
                if (trace) {
                  Trace.tearDownTrace();
                }
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
              setPrompt(client, reader, out);
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

  public static void setupCommands() {
    Builder<String, Command> builder = new ImmutableMap.Builder<String, Command>();
    register(builder, new HelpCommand());
    register(builder, new DebugCommand());
    register(builder, new TimedCommand());
    register(builder, new HighlightCommand());
    register(builder, new QuitCommand());
    register(builder, new ListTablesCommand());
    register(builder, new CreateTableCommand());
    register(builder, new EnableTableCommand());
    register(builder, new DisableTableCommand());
    register(builder, new RemoveTableCommand());
    register(builder, new DescribeTableCommand());
    register(builder, new CopyTableCommand());
    register(builder, new TableStatsCommand());
    register(builder, new SchemaTableCommand());
    register(builder, new QueryCommandOld());
    register(builder, new GetRowCommand());
    register(builder, new DeleteRowCommand());
    register(builder, new MutateRowCommand());
    register(builder, new TermsDataCommand());
    register(builder, new IndexAccessLogCommand());
    register(builder, new ShardClusterListCommand());
    register(builder, new ShardServerLayoutCommand());
    register(builder, new ControllersEchoCommand());
    register(builder, new ShardsEchoCommand());
    register(builder, new TruncateTableCommand());
    register(builder, new ClusterCommand());
    register(builder, new WaitInSafemodeCommand());
    register(builder, new TopCommand());
    register(builder, new ParseCommand());
    register(builder, new LoadTestDataCommand());
    register(builder, new SelectorCommand());
    register(builder, new AddColumnDefinitionCommand());
    register(builder, new ResetCommand());
    register(builder, new CreateSnapshotCommand());
    register(builder, new RemoveSnapshotCommand());
    register(builder, new ListSnapshotsCommand());
    register(builder, new DiscoverFileBufferSizeUtil());
    register(builder, new WhoAmICommand());
    register(builder, new UserCommand());
    register(builder, new TraceCommand());
    register(builder, new TraceList());
    register(builder, new TraceRemove());
    register(builder, new LogCommand());
    register(builder, new LogResetCommand());
    register(builder, new RemoveShardServerCommand());
    register(builder, new OptimizeTableCommand());
    register(builder, new QueryCommand());
    register(builder, new ListPlatformCommandsCommand());
    register(builder, new DescribePlatformCommandCommand());
    register(builder, new ExecutePlatformCommandCommand());
    register(builder, new ImportDataCommand());
    register(builder, new ListRunningPlatformCommandsCommand());
    register(builder, new CancelPlatformCommandCommand());
    register(builder, new WatchCommands());
    commands = builder.build();
  }

  private static void register(Builder<String, Command> builder, Command command) {
    builder.put(command.name(), command);
  }

  private static void setPrompt(Iface client, ConsoleReader reader, PrintWriter out) throws BlurException, TException,
      CommandException, IOException {
    List<String> shardClusterList;
    try {
      shardClusterList = client.shardClusterList();
    } catch (BlurException e) {
      out.println("Unable to retrieve cluster information - " + e.getMessage());
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
      cliShellOptions.setClient(BlurClient.getClient());
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
        cliShellOptions.setClient(BlurClient.getClient(arg0));
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
        cliShellOptions.setClient(BlurClient.getClient());
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

}
