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

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Level;

public class LogCommand extends Command {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length != 4) {
      throw new CommandException("Invalid args: " + help());
    }

    Iface clientToNode = BlurClient.getClient(args[1]);
    Level level = Level.valueOf(args[2].toUpperCase().trim());
    String classNameOrLoggerName = args[3].trim();
    if (classNameOrLoggerName.equals("ROOT")) {
      clientToNode.logging(null, level);
    } else {
      clientToNode.logging(classNameOrLoggerName, level);
    }
  }

  @Override
  public String description() {
    StringBuilder builder = new StringBuilder();
    for (Level level : Level.values()) {
      if (builder.length() != 0) {
        builder.append(", ");
      }
      builder.append(level.toString());
    }
    return "Change log levels of a server.  Levels are: " + builder.toString();
  }

  @Override
  public String usage() {
    return "<node:port> <log level> <logger name or class name>";
  }

  @Override
  public String name() {
    return "logger";
  }
}
