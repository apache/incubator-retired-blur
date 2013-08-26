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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class CreateTableCommandHelper {
  @SuppressWarnings("static-access")
  public static CommandLine parse(String[] otherArgs, Writer out) {
    Options options = new Options();
    options.addOption(
        OptionBuilder
        .withDescription("Disables the blockcache for this table. (Enabled by default)")
        .create("b"));
    
    options.addOption(
        OptionBuilder
        .withDescription("Disables the table when it is created. (Enabled by default)")
        .create("d"));
    
    options.addOption(
        OptionBuilder
        .withDescription("Enabled strict types on a table. (Disabled by default)")
        .create("s"));
    
    options.addOption(
        OptionBuilder
        .withDescription("Enables a read only table. (Disabled by default)")
        .create("r"));
    
    options.addOption(
        OptionBuilder
        .isRequired()
        .hasArg()
        .withArgName("tablename")
        .withDescription("* The table name.")
        .create("t"));
    
    options.addOption(
        OptionBuilder
        .isRequired()
        .hasArg()
        .withArgName("shard count")
        .withDescription("* The number of shards in the table.")
        .create("c"));
    
    options.addOption(
        OptionBuilder
        .hasArg()
        .withArgName("uri")
        .withDescription("The location of the table. (Example hdfs://namenode/blur/tables/table)")
        .create("l"));
    
    options.addOption(
        OptionBuilder
        .withArgName("filetype")
        .hasOptionalArgs()
        .withDescription("Sets the filetypes (.tim, .tis, .doc, etc.) to be cached in the block cache. (All by default)")
        .create("B"));
    
    options.addOption(
        OptionBuilder
        .withDescription("If table is not strict, disables the missing field, fieldless indexing. (Enabled by default)")
        .create("mfi"));
    
    options.addOption(
        OptionBuilder
        .withArgName("field type")
        .hasArg()
        .withDescription("If table is not strict, sets the field type for the missing field. (text by default)")
        .create("mft"));
    
    options.addOption(
        OptionBuilder
        .withArgName("name value")
        .hasArgs(2)
        .withDescription("If table is not strict, sets the properties for the missing field.")
        .create("mfp"));
    
    options.addOption(
        OptionBuilder
        .withArgName("name value")
        .hasArgs(2)
        .withDescription("Sets the properties for this table descriptor.")
        .create("p"));
    
    options.addOption(
        OptionBuilder
        .withArgName("column name*")
        .hasArgs()
        .withDescription("Sets what columns to pre cache during warmup. (By default all columns are cached)")
        .create("P"));
    
    options.addOption(
        OptionBuilder
        .withArgName("classname")
        .hasArg()
        .withDescription("Sets the similarity class for the table. (By org.apache.blur.lucene.search.FairSimilarity is used)")
        .create("S"));
    
    options.addOption(
        OptionBuilder
        .withDescription("Displays help for this command.")
        .create("h"));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, otherArgs);
      if (cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(out, true);
        formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "create", null, options,
            HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null, false);
        return null;
      }
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(out, true);
      formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "create", null, options,
          HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }
    return cmd;
  }
}
