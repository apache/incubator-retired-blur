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

import jline.Terminal;
import jline.console.ConsoleReader;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.CommandDescriptor;

public class ListPlatformCommandsCommand extends Command {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {

    List<CommandDescriptor> listInstalledCommands = client.listInstalledCommands();
    ConsoleReader reader = getConsoleReader();
    Terminal terminal = reader.getTerminal();
    int width = terminal.getWidth();
    int commandNameWidth = width / 4;
    int descriptionWidth = width - commandNameWidth;
    for (CommandDescriptor commandDescriptor : listInstalledCommands) {
      String commandName = commandDescriptor.getCommandName() + " ";
      out.print(addWhiteSpace(commandName, commandNameWidth));

      String description = commandDescriptor.getDescription();
      if (description == null) {
        out.println("-");
      } else {
        description = description.trim();
        while (description.length() > 0) {
          int len = Math.min(descriptionWidth, description.length());
          out.println(description.substring(0, len));
          description = description.substring(len);
          description = description.trim();
          if (description.length() > 0) {
            out.print(addWhiteSpace("", commandNameWidth));
          }
        }
      }
    }
  }

  @Override
  public String description() {
    return "List platform commands that are installed.";
  }

  @Override
  public String usage() {
    return "";
  }

  @Override
  public String name() {
    return "command-list";
  }
}
