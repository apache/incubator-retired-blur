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

import jline.console.ConsoleReader;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;

public abstract class Command {
  @SuppressWarnings("serial")
  public static class CommandException extends Exception {
    public CommandException(String msg) {
      super(msg);
    }
  }

  private ConsoleReader consoleReader;

  abstract public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException;

  public final String help() {
    return description() + " " + "Usage: " + name() + " " + usage();
  }

  abstract public String description();

  abstract public String usage();

  abstract public String name();

  public ConsoleReader getConsoleReader() {
    return consoleReader;
  }

  public void setConsoleReader(ConsoleReader consoleReader) {
    this.consoleReader = consoleReader;
  }

}
