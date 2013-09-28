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
import java.io.Writer;
import java.util.List;

import jline.Terminal;
import jline.console.ConsoleReader;

import org.apache.blur.shell.PagingPrintWriter.FinishedException;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class TermsDataCommand extends Command implements TableFirstArgCommand {
  @Override
  public void doit(PrintWriter outPw, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {

    if (args.length < 3) {
      throw new CommandException("Invalid args: " + help());
    }

    try {
      doitInternal(outPw, client, args);
    } catch (FinishedException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
    }
  }

  private void doitInternal(PrintWriter outPw, Blur.Iface client, String[] args) throws FinishedException,
      BlurException, TException {
    PagingPrintWriter out = new PagingPrintWriter(outPw);
    CommandLine cmd = parse(args, outPw);
    if (cmd == null) {
      return;
    }

    String tablename = args[1];
    String familyPlusColumn = args[2];
    String family = null;
    String column = familyPlusColumn;
    String startWith = "";
    short size = 100;

    if (familyPlusColumn.contains(".")) {
      int index = familyPlusColumn.indexOf(".");
      family = familyPlusColumn.substring(0, index);
      column = familyPlusColumn.substring(index + 1);
    }

    if (cmd.hasOption("n")) {
      size = Short.parseShort(cmd.getOptionValue("n"));
    }

    if (cmd.hasOption("s")) {
      startWith = cmd.getOptionValue("s");
    }

    boolean checkFreq = false;
    if (cmd.hasOption("F")) {
      checkFreq = true;
    }

    int maxWidth = 100;
    ConsoleReader reader = getConsoleReader();
    if (reader != null) {
      Terminal terminal = reader.getTerminal();
      maxWidth = terminal.getWidth() - 15;
      out.setLineLimit(terminal.getHeight() - 2);
    }

    List<String> terms = client.terms(tablename, family, column, startWith, size);
    for (int i = 0; i < terms.size(); i++) {
      if (checkFreq) {
        out.println(terms.get(i) + "\t" + client.recordFrequency(tablename, family, column, terms.get(i)));
      } else {
        out.println(terms.get(i));
      }
    }
  }

  @Override
  public String description() {
    return "Gets the terms list.";
  }

  @Override
  public String usage() {
    return "<tablename> <field> [-s <startwith>]  [-n <number>] [-F frequency]";
  }

  @Override
  public String name() {
    return "terms";
  }

  @SuppressWarnings("static-access")
  private static CommandLine parse(String[] otherArgs, Writer out) {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("startwith").hasArg().withDescription("The value to start with.")
        .create("s"));
    options.addOption(OptionBuilder.withArgName("size").hasArg().withDescription("The number of terms to return.")
        .create("n"));
    options.addOption(OptionBuilder.withDescription("Get the frequency of each term.").create("F"));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, otherArgs);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(out, true);
      formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "terms", null, options, HelpFormatter.DEFAULT_LEFT_PAD,
          HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }
    return cmd;
  }
}
