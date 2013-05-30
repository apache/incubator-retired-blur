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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;

import org.apache.blur.shell.Command.CommandException;
import org.apache.blur.shell.Main.QuitCommand.QuitCommandException;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TBinaryProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TFramedTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TSocket;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransport;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.BlurException;

import com.google.common.collect.ImmutableMap;

public class Main {
  /** is debugging enabled - off by default */
  static boolean debug = false;
  /** is timing enabled - off by default */
  static boolean timed = false;

  private static Map<String, Command> commands;
  
  public static void usage() {
    System.out.println("Usage: java " + Main.class.getName()
        + " controller:port");
  }

  private static class DebugCommand extends Command {

    @Override
    public void doit(PrintWriter out, Client client, String[] args)
        throws CommandException, TException, BlurException {
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
    public void doit(PrintWriter out, Client client, String[] args)
        throws CommandException, TException, BlurException {
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
    public void doit(PrintWriter out, Client client, String[] args)
        throws CommandException, TException, BlurException {
      out.println("Available commands:");
      for (Entry<String, Command> e: commands.entrySet()) {
        out.println("  " + e.getKey() + " - " + e.getValue().help());
      }
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
    public void doit(PrintWriter out, Client client, String[] args)
        throws CommandException, TException, BlurException {
      throw new QuitCommandException();
    }

    @Override
    public String help() {
      return "exit the shell";
    }
  }

  public static void main(String[] args) throws Throwable {
    commands = new ImmutableMap.Builder<String,Command>()
        .put("help", new HelpCommand())
        .put("debug", new DebugCommand())
        .put("timed", new TimedCommand())
        .put("quit", new QuitCommand())
        .put("listtables", new ListTablesCommand())
        .put("createtable", new CreateTableCommand())
        .put("enabletable", new EnableDisableTableCommand())
        .put("disabletable", new EnableDisableTableCommand())
        .put("removetable", new RemoveTableCommand())
        .put("describetable", new DescribeTableCommand())
        .put("tablestats", new TableStatsCommand())
        .put("schema", new SchemaTableCommand())
        .put("query", new QueryCommand())
        .put("getrow", new GetRowCommand())
        .put("mutaterow", new MutateRowCommand())
        .put("indexaccesslog", new IndexAccessLogCommand())
        .put("shardclusterlist", new ShardClusterListCommand())
        .put("shardserverlayout", new ShardServerLayoutCommand())
        .put("controllers", new ControllersEchoCommand())
        .build();

    try {
      ConsoleReader reader = new ConsoleReader();

      reader.setPrompt("blur> ");

      if ((args == null) || (args.length != 1)) {
        usage();
        return;
      }

      String[] hostport = args[0].split(":"); 

      if (hostport.length != 2) {
        usage();
        return;
      }

      List<Completer> completors = new LinkedList<Completer>();

      completors.add(new StringsCompleter(commands.keySet()));
      completors.add(new FileNameCompleter());

      for (Completer c : completors) {
        reader.addCompleter(c);
      }

      TTransport trans = new TSocket(hostport[0], Integer.parseInt(hostport[1]));
      TProtocol proto = new TBinaryProtocol(new TFramedTransport(trans));
      Client client = new Client(proto);
      try {
          trans.open();

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
                    out.println("Last command took "
                        + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
                        + "ms");
                  }
                }
              }
            }
          } finally {
            out.close();
          }
      } finally {
          trans.close();
      }
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }

}
