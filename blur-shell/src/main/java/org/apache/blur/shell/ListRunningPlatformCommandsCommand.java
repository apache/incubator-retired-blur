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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.CommandStatus;
import org.apache.blur.thrift.generated.CommandStatusState;
import org.apache.blur.thrift.generated.User;

public class ListRunningPlatformCommandsCommand extends Command {

  @Override
  public void doit(PrintWriter out, Iface client, String[] args) throws CommandException, TException, BlurException {
    if (args.length != 1) {
      throw new CommandException("Invalid args: " + help());
    }

    List<String> commandStatusList = client.commandStatusList(0, Short.MAX_VALUE);
    RunningSummary runningSummary = new RunningSummary();
    for (String id : commandStatusList) {
      CommandStatus commandStatus = client.commandStatus(id);
      Map<String, Map<CommandStatusState, Long>> serverStateMap = commandStatus.getServerStateMap();
      out.println(serverStateMap);
      Map<CommandStatusState, Long> summary = getSummary(serverStateMap);
      if (summary.containsKey(CommandStatusState.RUNNING)) {
        runningSummary.add(commandStatus, summary);
      }
    }

    runningSummary.print(out);
  }

  private Map<CommandStatusState, Long> getSummary(Map<String, Map<CommandStatusState, Long>> serverStateMap) {
    Map<CommandStatusState, Long> map = new HashMap<CommandStatusState, Long>();
    for (Map<CommandStatusState, Long> m : serverStateMap.values()) {
      for (Entry<CommandStatusState, Long> e : m.entrySet()) {
        Long c = map.get(e.getKey());
        Long newCount = e.getValue();
        if (c == null) {
          map.put(e.getKey(), newCount);
        } else {
          map.put(e.getKey(), c + newCount);
        }
      }
    }
    return map;
  }

  @Override
  public String description() {
    return "List the running commands on the cluster";
  }

  @Override
  public String usage() {
    return "";
  }

  @Override
  public String name() {
    return "command-running";
  }

  static class RunningSummary {

    private List<List<String>> _summary = new ArrayList<List<String>>();

    public void add(CommandStatus commandStatus, Map<CommandStatusState, Long> summary) {
      String executionId = commandStatus.getExecutionId();
      String commandName = commandStatus.getCommandName();
      User user = commandStatus.getUser();
      _summary.add(Arrays.asList(executionId, commandName, user.getUsername(), toString(summary)));
    }

    private String toString(Map<CommandStatusState, Long> summary) {
      StringBuilder builder = new StringBuilder();
      for (Entry<CommandStatusState, Long> e : summary.entrySet()) {
        if (builder.length() != 0) {
          builder.append(',');
        }
        builder.append(e.getKey().name()).append(":").append(e.getValue());
      }
      return builder.toString();
    }

    public void print(PrintWriter out) {
      if (_summary == null || _summary.isEmpty()) {
        out.println("No Commands Running");
        return;
      }
      int[] maxLengths = getMaxLength();
      for (List<String> line : _summary) {
        print(out, line, maxLengths);
      }
    }

    private void print(PrintWriter out, List<String> line, int[] maxLengths) {
      for (int i = 0; i < line.size(); i++) {
        int length = maxLengths[i];
        print(out, line.get(i), length);
      }
      out.println();
    }

    private void print(PrintWriter out, String s, int length) {
      out.print(buffer(s, length));
    }

    private String buffer(String s, int length) {
      while (s.length() < length) {
        s = s + " ";
      }
      return s;
    }

    private int[] getMaxLength() {
      int[] len = null;
      for (List<String> line : _summary) {
        if (len == null) {
          len = new int[line.size()];
        }
        for (int i = 0; i < line.size(); i++) {
          if (line.get(i).length() > len[i]) {
            len[i] = line.get(i).length();
          }
        }
      }
      // Add space between
      for (int i = 0; i < len.length; i++) {
        len[i]++;
      }
      return len;
    }
  }
}
