/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import jline.console.ConsoleReader;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.CommandStatus;
import org.apache.blur.thrift.generated.CommandStatusState;
import org.apache.blur.thrift.generated.User;

public class WatchCommands extends Command {

  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    ConsoleReader reader = this.getConsoleReader();
    try {
      doitInternal(client, reader);
    } catch (IOException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
      throw new CommandException(e.getMessage());
    } finally {
      if (reader != null) {
        reader.setPrompt(Main.PROMPT);
      }
    }
  }

  private static void doitInternal(Iface client, ConsoleReader reader) throws IOException, TException, CommandException {
    TableDisplay tableDisplay = new TableDisplay(reader);
    tableDisplay.setSeperator("|");
    tableDisplay.setHeader(0, "id");
    tableDisplay.setHeader(1, "command");
    tableDisplay.setHeader(2, "user");
    tableDisplay.setHeader(3, "summary");

    final AtomicBoolean running = new AtomicBoolean(true);
    tableDisplay.addKeyHook(new Runnable() {
      @Override
      public void run() {
        synchronized (running) {
          running.set(false);
          running.notifyAll();
        }
      }
    }, 'q');

    try {
      int maxL = 0;
      while (running.get()) {

        List<String> commandStatusList = client.commandStatusList(0, Short.MAX_VALUE);
        List<String[]> display = new ArrayList<String[]>();
        for (String id : commandStatusList) {
          CommandStatus commandStatus;
          try {
            commandStatus = client.commandStatus(id);
          } catch (BlurException e) {
            String message = e.getMessage();
            if (message != null && message.startsWith("NOT_FOUND")) {
              commandStatus = null;
            } else {
              throw e;
            }
          }
          if (commandStatus == null) {
            continue;
          }
          Map<String, Map<CommandStatusState, Long>> serverStateMap = commandStatus.getServerStateMap();
          Map<CommandStatusState, Long> summary = ListRunningPlatformCommandsCommand.getSummary(serverStateMap);
          String executionId = commandStatus.getExecutionId();
          String commandName = commandStatus.getCommandName();
          User user = commandStatus.getUser();
          if (summary.containsKey(CommandStatusState.RUNNING)) {
            String stringSummary = ListRunningPlatformCommandsCommand.toStringSummary(summary);
            display.add(new String[] { executionId, commandName, user.toString(), stringSummary });
          } else if (summary.containsKey(CommandStatusState.INTERRUPTED)) {
            display
                .add(new String[] { executionId, commandName, user.toString(), CommandStatusState.INTERRUPTED.name() });
          } else {
            display.add(new String[] { executionId, commandName, user.toString(), CommandStatusState.COMPLETE.name() });
          }
        }

        int l = 0;
        for (String[] array : display) {
          tableDisplay.set(0, l, array[0]);
          tableDisplay.set(1, l, array[1]);
          tableDisplay.set(2, l, array[2]);
          tableDisplay.set(3, l, array[3]);
          l++;
        }
        if (l > maxL) {
          maxL = l;
        }
        Thread.sleep(3000);
      }
    } catch (InterruptedException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
      throw new CommandException(e.getMessage());
    } finally {
      tableDisplay.close();
    }
  }

  @Override
  public String description() {
    return "Watch commands execute.";
  }

  @Override
  public String usage() {
    return "";
  }

  @Override
  public String name() {
    return "command-watch";
  }
}
