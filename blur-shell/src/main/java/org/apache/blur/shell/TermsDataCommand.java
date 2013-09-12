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

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class TermsDataCommand extends Command implements TableFirstArgCommand {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {

    if (args.length < 3) {
      throw new CommandException("Invalid args: " + help());
    }

    CommandLine cmd = parse(args, out);
    /*
    #then this starts displaying all the row ids in the system
    terms testtable rowid
    #This command starts displaying all the terms for column "col1" in family "fam0"in the system
    terms testtable fam0.col1
    #This command starts displaying all the terms for column "col1" in family "fam0"in the system starting at "value1"
    terms testtable fam0.col1 -s value1
    #This command starts displaying all the terms for column "col1" in family "fam0"in the system starting at "value1" fetch 100 then exit
    terms testtable fam0.col1 -s value1 -n 100
    */
    String tablename = args[1];
    String namePlusValue = args[2];
    String family = null;
    String column = namePlusValue;
    String startWith = "";
    short size = 100;
    
      
    if (namePlusValue.contains(".")){
      int index = namePlusValue.indexOf(".");
      family = namePlusValue.substring(0, index);
      column = namePlusValue.substring(index + 1);
    }

    if (cmd.hasOption("n")){
      size = Short.parseShort(cmd.getOptionValue("n"));
    }

    if (cmd.hasOption("s")){
      startWith = cmd.getOptionValue("s");
      out.println("startWith:"+startWith);
    }

    boolean checkFreq = false;
    if (cmd.hasOption("F")){
      checkFreq = true;
    }

    //todo print line by line. also break input at certain amount
    List<String> terms = client.terms(tablename,family,column,startWith,size);
    for (int i=0;i<terms.size(); i++){
      if (checkFreq){
          out.println(terms.get(i)+"\t"+client.recordFrequency(tablename,family,column,terms.get(i)));
      }
      else
      {
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
    options.addOption(OptionBuilder.withArgName("startwith").hasArg()
        .withDescription("The value to start with.").create("s"));
    options.addOption(OptionBuilder.withArgName("size").hasArg()
        .withDescription("The number of terms to return.").create("n"));
    options.addOption(OptionBuilder.withDescription("The frequency of each term.").create("F"));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, otherArgs);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(out, true);
      formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "terms", null, options,
          HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }
    return cmd;
  }
}
