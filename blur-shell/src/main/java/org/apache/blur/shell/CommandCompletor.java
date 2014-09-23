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

import static jline.internal.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import jline.console.completer.Completer;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.CommandDescriptor;

public class CommandCompletor implements Completer {

  private final Map<String, Command> _commands;
  private final Iface _client;

  public CommandCompletor(Map<String, Command> commands, Iface client) {
    _commands = commands;
    _client = client;
  }

  @Override
  public int complete(final String buf, final int cursor, final List<CharSequence> candidates) {
    // buffer could be null
    checkNotNull(candidates);
    SortedSet<String> strings = sort(_commands.keySet());
    if (buf == null) {
      candidates.addAll(strings);
    } else {
      String buffer = buf.substring(0, cursor);
      List<String> partialTableNames = isFirstArgPartialTableName(buffer);
      if (partialTableNames != null && !partialTableNames.isEmpty()) {
        candidates.addAll(partialTableNames);
      } else {
        for (String match : strings.tailSet(buffer)) {
          if (!match.startsWith(buffer)) {
            break;
          }
          candidates.add(match);
        }
      }
    }
    if (candidates.size() == 1) {
      candidates.set(0, candidates.get(0) + " ");
    }
    return candidates.isEmpty() ? -1 : 0;
  }

  private List<String> isFirstArgPartialTableName(String buffer) {
    String[] args = buffer.split("\\s+");
    if (args.length > 2) {
      return null;
    }
    String command = args[0];
    Command cmd = _commands.get(command);
    if (cmd == null) {
      return null;
    }
    if (cmd instanceof FirstArgCommand) {
      String partial = "";
      if (args.length == 2) {
        partial = args[1];
      }
      if (Main.cluster != null) {
        try {
          List<String> list = getOptions(cmd);
          Collections.sort(list);
          List<String> results = new ArrayList<String>();
          for (String option : list) {
            if (option.startsWith(partial)) {
              results.add(args[0] + " " + option);
            }
          }
          return results;
        } catch (BlurException e) {
          if (Main.debug) {
            e.printStackTrace();
          }
          return null;
        } catch (TException e) {
          if (Main.debug) {
            e.printStackTrace();
          }
          return null;
        }
      }
    }
    return null;
  }

  private List<String> getOptions(Command cmd) throws BlurException, TException {
    if (cmd instanceof TableFirstArgCommand) {
      return new ArrayList<String>(_client.tableListByCluster(Main.cluster));
    } else if (cmd instanceof CommandFirstArgCommand) {
      List<CommandDescriptor> listInstalledCommands = _client.listInstalledCommands();
      List<String> list = new ArrayList<String>();
      for (CommandDescriptor commandDescriptor : listInstalledCommands) {
        list.add(commandDescriptor.getCommandName());
      }
      return list;
    } else {
      return new ArrayList<String>();
    }
  }

  private SortedSet<String> sort(Set<String> set) {
    return new TreeSet<String>(set);
  }
}
