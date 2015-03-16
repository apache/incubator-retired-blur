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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
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
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.Splitter;

@SuppressWarnings("static-access")
public class CsvBlurDriver {

  private static final Log LOG = LogFactory.getLog(CsvBlurDriver.class);

  public static final String CSVLOADER = "csvloader";
  public static final String MAPRED_COMPRESS_MAP_OUTPUT = "mapred.compress.map.output";
  public static final String MAPRED_MAP_OUTPUT_COMPRESSION_CODEC = "mapred.map.output.compression.codec";
  public static final int DEFAULT_WIDTH = 100;
  public static final String HEADER = "The \""
      + CSVLOADER
      + "\" command is used to load delimited into a Blur table.\nThe required options are \"-c\", \"-t\", \"-d\". The "
      + "standard format for the contents of a file is:\"rowid,recordid,family,col1,col2,...\". However there are "
      + "several options, such as the rowid and recordid can be generated based on the data in the record via the "
      + "\"-A\" and \"-a\" options. The family can assigned based on the path via the \"-I\" option. The column "
      + "name order can be mapped via the \"-d\" option. Also you can set the input "
      + "format to either sequence files vie the \"-S\" option or leave the default text files.";

  enum COMPRESSION {
    SNAPPY(SnappyCodec.class), GZIP(GzipCodec.class), BZIP(BZip2Codec.class), DEFAULT(DefaultCodec.class);

    private final String className;

    private COMPRESSION(Class<? extends CompressionCodec> clazz) {
      className = clazz.getName();
    }

    public String getClassName() {
      return className;
    }
  }

  interface ControllerPool {
    Iface getClient(String controllerConnectionStr);
  }

  public static void main(String... args) throws Exception {
    Configuration configuration = new Configuration();
    String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
    AtomicReference<Callable<Void>> ref = new AtomicReference<Callable<Void>>();
    Job job = setupJob(configuration, new ControllerPool() {
      @Override
      public Iface getClient(String controllerConnectionStr) {
        return BlurClient.getClient(controllerConnectionStr);
      }
    }, ref, otherArgs);
    if (job == null) {
      System.exit(1);
    }
    boolean waitForCompletion = job.waitForCompletion(true);
    if (waitForCompletion) {
      Callable<Void> callable = ref.get();
      if (callable != null) {
        callable.call();
      }
    }
    System.exit(waitForCompletion ? 0 : 1);
  }

  public static Job setupJob(Configuration configuration, ControllerPool controllerPool,
      AtomicReference<Callable<Void>> ref, String... otherArgs) throws Exception {
    CommandLine cmd = parse(otherArgs);
    if (cmd == null) {
      return null;
    }

    final String controllerConnectionStr = cmd.getOptionValue("c");
    final String tableName = cmd.getOptionValue("t");

    final Iface client = controllerPool.getClient(controllerConnectionStr);
    TableDescriptor tableDescriptor = client.describe(tableName);

    Job job = Job.getInstance(configuration, "Blur indexer [" + tableName + "]");
    job.setJarByClass(CsvBlurDriver.class);
    job.setMapperClass(CsvBlurMapper.class);

    if (cmd.hasOption("p")) {
      job.getConfiguration().set(MAPRED_COMPRESS_MAP_OUTPUT, "true");
      String codecStr = cmd.getOptionValue("p");
      COMPRESSION compression;
      try {
        compression = COMPRESSION.valueOf(codecStr.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        compression = null;
      }
      if (compression == null) {
        job.getConfiguration().set(MAPRED_MAP_OUTPUT_COMPRESSION_CODEC, codecStr.trim());
      } else {
        job.getConfiguration().set(MAPRED_MAP_OUTPUT_COMPRESSION_CODEC, compression.getClassName());
      }
    }
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
      if (cmd.hasOption("C")) {
        System.err.println("'I' and 'C' both parameters can not be used together.");
        return null;
      }
      Option[] options = cmd.getOptions();
      for (Option option : options) {
        if (option.getOpt().equals("I")) {
          String[] values = option.getValues();
          if (values.length < 2) {
            System.err.println("'I' parameter missing minimum args of (family path*)");
            return null;
          }
          for (String p : getSubArray(values, 1)) {
            Path path = new Path(p);
            CsvBlurMapper.addFamilyPath(job, values[0], path);
            FileInputFormat.addInputPath(job, path);
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
    BlurMapReduceUtil.addDependencyJars(job.getConfiguration(), Splitter.class);
    if (cmd.hasOption("r")) {
      int reducerMultiplier = Integer.parseInt(cmd.getOptionValue("r"));
      BlurOutputFormat.setReducerMultiplier(job, reducerMultiplier);
    }
    final Path output;
    if (cmd.hasOption("out")) {
      output = new Path(cmd.getOptionValue("out"));
    } else {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      String userName = currentUser.getUserName();
      output = new Path("/user/" + userName + "/.blur-" + System.currentTimeMillis());
    }
    BlurOutputFormat.setOutputPath(job, output);
    if (cmd.hasOption("import")) {
      ref.set(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          client.loadData(tableName, output.toUri().toString());
          return null;
        }
      });
    }
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
    if (!fileSystem.exists(path)) {
      LOG.warn("Path not found [{0}]", path);
      return pathSet;
    }
    FileStatus[] listStatus = fileSystem.listStatus(path);
    for (FileStatus status : listStatus) {
      if (status.isDirectory()) {
        pathSet.addAll(recurisvelyGetPathesContainingFiles(status.getPath(), configuration));
      } else {
        pathSet.add(status.getPath().getParent());
      }
    }
    return pathSet;
  }

  private static CommandLine parse(String... otherArgs) throws ParseException {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("controller*").hasArgs().isRequired(true)
        .withDescription("* Thrift controller connection string. (host1:40010 host2:40010 ...)").create("c"));
    options.addOption(OptionBuilder.withArgName("tablename").hasArg().isRequired(true)
        .withDescription("* Blur table name.").create("t"));
    options.addOption(OptionBuilder.withArgName("family column*").hasArgs().isRequired(true)
        .withDescription("* Define the mapping of fields in the CSV file to column names. (family col1 col2 col3 ...)")
        .create("d"));
    options.addOption(OptionBuilder
        .withArgName("delimiter")
        .hasArg()
        .withDescription(
            "The file delimiter to be used. (default value ',')  NOTE: For special "
                + "charactors like the default hadoop separator of ASCII value 1, you can use standard "
                + "java escaping (\\u0001)").create("s"));
    options
        .addOption(OptionBuilder
            .withArgName("path*")
            .hasArg()
            .withDescription(
                "The directory to index, the family name is assumed to BE present in the file contents. (hdfs://namenode/input/in1)")
            .create("i"));
    options
        .addOption(OptionBuilder
            .withArgName("family path*")
            .hasArgs()
            .withDescription(
                "The directory to index with a family name, the family name is assumed to NOT be present in the file contents. (family hdfs://namenode/input/in1)")
            .create("I"));
    options
        .addOption(OptionBuilder
            .withArgName("auto generate record ids")
            .withDescription(
                "No Record Ids - Automatically generate record ids for each record based on a MD5 has of the data within the record.")
            .create("a"));
    options
        .addOption(OptionBuilder
            .withArgName("auto generate row ids")
            .withDescription(
                "No Row Ids - Automatically generate row ids for each record based on a MD5 has of the data within the record.")
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
        .withArgName("size")
        .hasArg()
        .withDescription(
            "The maximum number of Lucene documents to buffer in the reducer for a single "
                + "row before spilling over to disk. (default 1000)").create("b"));
    options.addOption(OptionBuilder
        .withArgName("multiplier")
        .hasArg()
        .withDescription(
            "The reducer multipler allows for an increase in the number of reducers per "
                + "shard in the given table.  For example if the table has 128 shards and the "
                + "reducer multiplier is 4 the total number of reducers will be 512, 4 reducers "
                + "per shard. (default 1)").create("r"));
    options.addOption(OptionBuilder
        .withArgName("minimum maximum")
        .hasArgs(2)
        .withDescription(
            "Enables a combine file input to help deal with many small files as the input. Provide "
                + "the minimum and maximum size per mapper.  For a minimum of 1GB and a maximum of "
                + "2.5GB: (1000000000 2500000000)").create("C"));
    options.addOption(OptionBuilder
        .withArgName("codec")
        .hasArgs(1)
        .withDescription(
            "Sets the compression codec for the map compress output setting. (SNAPPY,GZIP,BZIP,DEFAULT, or classname)")
        .create("p"));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Sets the output directory for the map reduce job before the indexes are loaded into Blur.")
        .create("out"));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Imports the data into Blur after the map reduce job completes.")
        .create("import"));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, otherArgs);
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(System.err, true);
      formatter.printHelp(pw, DEFAULT_WIDTH, CSVLOADER, HEADER, options, HelpFormatter.DEFAULT_LEFT_PAD,
          HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }

    if (!(cmd.hasOption("I") || cmd.hasOption("i"))) {
      System.err.println("Missing input directory, see options 'i' and 'I'.");
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(System.err, true);
      formatter.printHelp(pw, DEFAULT_WIDTH, CSVLOADER, HEADER, options, HelpFormatter.DEFAULT_LEFT_PAD,
          HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }
    return cmd;
  }

  public static class CsvBlurCombineSequenceFileInputFormat extends CombineFileInputFormat<Writable, Text> {

    private static class SequenceFileRecordReaderWrapper extends RecordReader<Writable, Text> {

      private final RecordReader<Writable, Text> delegate;
      private final FileSplit fileSplit;

      @SuppressWarnings("unused")
      public SequenceFileRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer index)
          throws IOException {
        fileSplit = new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index),
            split.getLocations());
        delegate = new SequenceFileInputFormat<Writable, Text>().createRecordReader(fileSplit, context);
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return delegate.getProgress();
      }

      @Override
      public Writable getCurrentKey() throws IOException, InterruptedException {
        return delegate.getCurrentKey();
      }

      @Override
      public Text getCurrentValue() throws IOException, InterruptedException {
        return delegate.getCurrentValue();
      }

      @Override
      public void initialize(InputSplit arg0, TaskAttemptContext context) throws IOException, InterruptedException {
        delegate.initialize(fileSplit, context);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return delegate.nextKeyValue();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }

    }

    @Override
    public RecordReader<Writable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException {
      return new CombineFileRecordReader<Writable, Text>((CombineFileSplit) split, context,
          SequenceFileRecordReaderWrapper.class);
    }
  }

}
