package org.apache.blur.mapreduce.lib;

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
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("static-access")
public class CsvBlurDriver {

  interface ControllerPool {
    Iface getClient(String controllerConnectionStr);
  }

  public static void main(String... args) throws Exception {

    Configuration configuration = new Configuration();
    String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

    Job job = setupJob(configuration, new ControllerPool() {
      @Override
      public Iface getClient(String controllerConnectionStr) {
        return BlurClient.getClient(controllerConnectionStr);
      }
    }, otherArgs);
    if (job == null) {
      System.exit(1);
    }

    boolean waitForCompletion = job.waitForCompletion(true);
    System.exit(waitForCompletion ? 0 : 1);
  }

  public static Job setupJob(Configuration configuration, ControllerPool controllerPool, String... otherArgs)
      throws Exception {
    CommandLine cmd = parse(otherArgs);
    if (cmd == null) {
      return null;
    }

    final String controllerConnectionStr = cmd.getOptionValue("c");
    final String tableName = cmd.getOptionValue("t");

    final Iface client = controllerPool.getClient(controllerConnectionStr);
    TableDescriptor tableDescriptor = client.describe(tableName);

    Job job = new Job(configuration, "Blur indexer [" + tableName + "]");
    job.setJarByClass(CsvBlurDriver.class);
    job.setMapperClass(CsvBlurMapper.class);

    if (cmd.hasOption("a")) {
      CsvBlurMapper.setAutoGenerateRecordIdAsHashOfData(job, true);
    }
    if (cmd.hasOption("A")) {
      CsvBlurMapper.setAutoGenerateRowIdAsHashOfData(job, true);
    }
    if (cmd.hasOption("S")) {
      job.setInputFormatClass(SequenceFileInputFormat.class);
    } else {
      job.setInputFormatClass(TextInputFormat.class);
    }
    
    if (cmd.hasOption("C")) {
      if (cmd.hasOption("S")) {
        String[] optionValues = cmd.getOptionValues("C");
        job.setInputFormatClass(CsvBlurCombineSequenceFileInputFormat.class);
        CombineFileInputFormat.setMinInputSplitSize(job, Long.parseLong(optionValues[0]));
        CombineFileInputFormat.setMaxInputSplitSize(job, Long.parseLong(optionValues[1]));
      } else {
        System.err.println("'C' can only be used with option 'S'");
        return null;
      }
    }

    if (cmd.hasOption("i")) {
      for (String input : cmd.getOptionValues("i")) {
        Path path = new Path(input);
        Set<Path> pathSet = recurisvelyGetPathesContainingFiles(path, job.getConfiguration());
        if (pathSet.isEmpty()) {
          FileInputFormat.addInputPath(job, path);
        } else {
          for (Path p : pathSet) {
            FileInputFormat.addInputPath(job, p);
          }
        }
      }
    }
    // processing the 'I' option
    if (cmd.hasOption("I")) {
      Option[] options = cmd.getOptions();
      for (Option option : options) {
        if (option.getOpt().equals("I")) {
          String[] values = option.getValues();
          if (values.length < 2) {
            System.err.println("'I' parameter missing minimum args of (family path*)");
            return null;
          }
          for (String p : getSubArray(values, 1)) {
            CsvBlurMapper.addFamilyPath(job, values[0], new Path(p));
          }
        }
      }
    }

    if (cmd.hasOption("s")) {
      CsvBlurMapper.setSeparator(job, StringEscapeUtils.unescapeJava(cmd.getOptionValue("s")));
    }
    if (cmd.hasOption("o")) {
      BlurOutputFormat.setOptimizeInFlight(job, false);
    }
    if (cmd.hasOption("l")) {
      BlurOutputFormat.setIndexLocally(job, false);
    }
    if (cmd.hasOption("b")) {
      int maxDocumentBufferSize = Integer.parseInt(cmd.getOptionValue("b"));
      BlurOutputFormat.setMaxDocumentBufferSize(job, maxDocumentBufferSize);
    }
    if (cmd.hasOption("r")) {
      int reducerMultiplier = Integer.parseInt(cmd.getOptionValue("r"));
      BlurOutputFormat.setReducerMultiplier(job, reducerMultiplier);
    }
    // processing the 'd' option
    Option[] options = cmd.getOptions();
    for (Option option : options) {
      if (option.getOpt().equals("d")) {
        String[] values = option.getValues();
        if (values.length < 2) {
          System.err.println("'d' parameter missing minimum args of (family columname*)");
          return null;
        }
        CsvBlurMapper.addColumns(job, values[0], getSubArray(values, 1));
      }
    }
    BlurOutputFormat.setupJob(job, tableDescriptor);
    return job;
  }

  private static String[] getSubArray(String[] array, int starting) {
    String[] result = new String[array.length - starting];
    System.arraycopy(array, starting, result, 0, result.length);
    return result;
  }

  private static Set<Path> recurisvelyGetPathesContainingFiles(Path path, Configuration configuration)
      throws IOException {
    Set<Path> pathSet = new HashSet<Path>();
    FileSystem fileSystem = path.getFileSystem(configuration);
    FileStatus[] listStatus = fileSystem.listStatus(path);
    for (FileStatus status : listStatus) {
      if (status.isDir()) {
        pathSet.addAll(recurisvelyGetPathesContainingFiles(status.getPath(), configuration));
      } else {
        pathSet.add(status.getPath().getParent());
      }
    }
    return pathSet;
  }

  private static CommandLine parse(String... otherArgs) throws ParseException {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("controller").hasArgs().isRequired(true)
        .withDescription("* Thrift controller connection string. (host1:40010 host2:40010 ...)").create("c"));
    options.addOption(OptionBuilder.withArgName("tablename").hasArg().isRequired(true)
        .withDescription("* Blur table name.").create("t"));
    options.addOption(OptionBuilder.withArgName("family and column definitions").hasArgs().isRequired(true)
        .withDescription("* Define the mapping of fields in the CSV file to column names. (family col1 col2 col3 ...)")
        .create("d"));
    options.addOption(OptionBuilder
        .withArgName("file delimiter")
        .hasArg()
        .withDescription(
            "The file delimiter to be used. (default value ',')  NOTE: For special "
                + "charactors like the default hadoop separator of ASCII value 1, you can use standard "
                + "java escaping (\\u0001)").create("s"));
    options.addOption(OptionBuilder.withArgName("file input").hasArg()
        .withDescription("The directory to index. (hdfs://namenode/input/in1)").create("i"));
    options.addOption(OptionBuilder.withArgName("file input").hasArgs()
        .withDescription("The directory to index with family name. (family hdfs://namenode/input/in1)").create("I"));
    options.addOption(OptionBuilder
        .withArgName("auto generate record ids")
        .withDescription(
            "Automatically generate record ids for each record based on a MD5 has of the data within the record.")
        .create("a"));
    options.addOption(OptionBuilder
        .withArgName("auto generate row ids")
        .withDescription(
            "Automatically generate row ids for each record based on a MD5 has of the data within the record.")
        .create("A"));
    options.addOption(OptionBuilder.withArgName("disable optimize indexes during copy")
        .withDescription("Disable optimize indexes during copy, this has very little overhead. (enabled by default)")
        .create("o"));
    options.addOption(OptionBuilder
        .withArgName("disable index locally")
        .withDescription(
            "Disable the use storage local on the server that is running the reducing "
                + "task and copy to Blur table once complete. (enabled by default)").create("l"));
    options.addOption(OptionBuilder.withArgName("sequence files inputs")
        .withDescription("The input files are sequence files.").create("S"));
    options.addOption(OptionBuilder
        .withArgName("lucene document buffer size")
        .hasArg()
        .withDescription(
            "The maximum number of Lucene documents to buffer in the reducer for a single "
                + "row before spilling over to disk. (default 1000)").create("b"));
    options.addOption(OptionBuilder
        .withArgName("reducer multiplier")
        .hasArg()
        .withDescription(
            "The reducer multipler allows for an increase in the number of reducers per "
                + "shard in the given table.  For example if the table has 128 shards and the "
                + "reducer multiplier is 4 the total number of redcuer will be 512, 4 reducers "
                + "per shard. (default 1)").create("r"));
    options.addOption(OptionBuilder
        .withArgName("minimum maximum")
        .hasArgs(2)
        .withDescription(
            "Enables a combine file input to help deal with many small files as the input. Provide " +
            "the minimum and maximum size per mapper.  For a minimum of 1GB and a maximum of " +
            "2.5GB: (1000000000 2500000000)").create("C"));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, otherArgs);
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(System.err, true);
      formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "csvindexer", null, options, HelpFormatter.DEFAULT_LEFT_PAD,
          HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }

    if (!(cmd.hasOption("I") || cmd.hasOption("i"))) {
      System.err.println("Missing input directory, see options 'i' and 'I'.");
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(System.err, true);
      formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "csvindexer", null, options, HelpFormatter.DEFAULT_LEFT_PAD,
          HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }
    return cmd;
  }

  public static class CsvBlurCombineSequenceFileInputFormat extends CombineFileInputFormat<Writable, Text> {

    @Override
    public RecordReader<Writable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException {
      return new SequenceFileRecordReader<Writable, Text>();
    }

  }
}
