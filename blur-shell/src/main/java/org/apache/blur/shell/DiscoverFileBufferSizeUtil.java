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
package org.apache.blur.shell;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DiscoverFileBufferSizeUtil extends Command {

  @Override
  public void doit(PrintWriter out, Iface client, String[] args) throws CommandException, TException, BlurException {
    CommandLine cmd = parse(args, out);
    if (cmd == null) {
      throw new CommandException(name() + " missing required arguments");
    }
    Option[] options = cmd.getOptions();
    for (Option option : options) {
      out.println(option);
      String opt = option.getOpt();
      String value = option.getValue();
      System.out.println(opt + " " + value);

    }

    Path path = new Path(cmd.getOptionValue("p"));
    long size = Long.parseLong(cmd.getOptionValue("s", "1000000000"));// 1GB
    int sampleSize = Integer.parseInt(cmd.getOptionValue("S", "10"));// 10
    int warmupSize = Integer.parseInt(cmd.getOptionValue("W", "3"));// 3
    int min = Integer.parseInt(cmd.getOptionValue("n", "12"));// 12
    int max = Integer.parseInt(cmd.getOptionValue("x", "19"));// 19
    int readSamplesPerPass = Integer.parseInt(cmd.getOptionValue("r", "1000"));// 1000

    Configuration configuration = new Configuration();
    FileSystem fileSystem;
    try {
      fileSystem = path.getFileSystem(configuration);
    } catch (IOException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
      throw new CommandException(e.getMessage());
    }
    Random random = new Random();
    Map<Integer, Long> writingResults;
    try {
      writingResults = runWritingTest(out, path, fileSystem, random, sampleSize, warmupSize, size, min, max);
    } catch (IOException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
      throw new CommandException(e.getMessage());
    }
    Map<Integer, Long> readingResults;
    try {
      readingResults = runReadingTest(out, path, fileSystem, random, sampleSize, warmupSize, size, min, max,
          readSamplesPerPass);
    } catch (IOException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
      throw new CommandException(e.getMessage());
    }

    out.println("Printing Results for Writing");
    printResults(out, writingResults);

    out.println("Printing Results for Reading");
    printResults(out, readingResults);
  }

  @SuppressWarnings("static-access")
  public CommandLine parse(String[] otherArgs, Writer out) {
    Options options = new Options();
    options.addOption(OptionBuilder.withDescription("The path to write temp files.").hasArg().isRequired(true)
        .create("p"));
    options.addOption(OptionBuilder.withDescription("The size of the temp files (1 GB by default).").hasArg()
        .create("s"));
    options.addOption(OptionBuilder.withDescription("Number of cycles of the test. (10 by default)").hasArg()
        .create("S"));
    options.addOption(OptionBuilder.withDescription("Number of warmup cycles. (3 by default)").hasArg().create("W"));
    options.addOption(OptionBuilder.withDescription("Min buffer size power of 2 (12 by default 4KB)").hasArg()
        .create("n"));
    options.addOption(OptionBuilder.withDescription("Max buffer size power of 2 (19 by default 512KB)").hasArg()
        .create("x"));
    options.addOption(OptionBuilder
        .withDescription("Number of random read samples during read test. (1000 by default)").hasArg().create("r"));
    options.addOption(OptionBuilder.withDescription("Displays help for this command.").create("h"));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, otherArgs);
      if (cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(out, true);
        formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, name(), null, options, HelpFormatter.DEFAULT_LEFT_PAD,
            HelpFormatter.DEFAULT_DESC_PAD, null, false);
        return null;
      }
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(out, true);
      formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, name(), null, options, HelpFormatter.DEFAULT_LEFT_PAD,
          HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }
    return cmd;
  }

  @Override
  public String description() {
    return "Tune the buffer sizes for your filesystem.  Run -h for full argument list.";
  }

  @Override
  public String usage() {
    return "-p <tmp path>";
  }

  @Override
  public String name() {
    return "fstune";
  }

  private static void printResults(PrintWriter out, Map<Integer, Long> results) {
    int bestBuffer = getTheBest(results);
    for (Entry<Integer, Long> e : results.entrySet()) {
      int bufSize = e.getKey();
      Long time = e.getValue();
      if (bufSize == bestBuffer) {
        out.print("*");
      } else {
        out.print(" ");
      }
      out.println(" Buffer size [" + bufSize + "] took [" + (time) / 1000000.0 + " ms]");
      out.flush();
    }
  }

  private static Map<Integer, Long> runReadingTest(PrintWriter out, Path path, FileSystem fileSystem, Random random,
      int sampleSize, int warmupSize, long size, int min, int max, int readSamples) throws IOException {
    out.println("Warming up JVM for Reading.");
    out.flush();
    for (int i = 0; i < warmupSize; i++) {
      testReadBuffer(out, "Warmup Read Pass [" + i + "]", random, fileSystem, path, size, min, max, readSamples);
    }
    Map<Integer, Long> readingResults = new TreeMap<Integer, Long>();
    for (int s = 0; s < sampleSize; s++) {
      add(readingResults,
          testReadBuffer(out, "Read Pass [" + s + "]", random, fileSystem, path, size, min, max, readSamples));
    }
    return readingResults;
  }

  private static Map<Integer, Long> runWritingTest(PrintWriter out, Path path, FileSystem fileSystem, Random random,
      int sampleSize, int warmupSize, long size, int min, int max) throws IOException {
    out.println("Warming up JVM for Writing.");
    out.flush();
    for (int i = 0; i < warmupSize; i++) {
      testWriteBuffer(out, "Warmup Write Pass [" + i + "]", random, fileSystem, path, size, min, max);
    }
    Map<Integer, Long> writingResults = new TreeMap<Integer, Long>();
    for (int s = 0; s < sampleSize; s++) {
      add(writingResults, testWriteBuffer(out, "Write Pass [" + s + "]", random, fileSystem, path, size, min, max));
    }
    return writingResults;
  }

  private static void add(Map<Integer, Long> results, Map<Integer, Long> newResults) {
    for (Entry<Integer, Long> e : newResults.entrySet()) {
      Long value = results.get(e.getKey());
      if (value == null) {
        results.put(e.getKey(), e.getValue());
      } else {
        results.put(e.getKey(), e.getValue() + value);
      }
    }
  }

  private static Integer getTheBest(Map<Integer, Long> results) {
    long lowestTime = Long.MAX_VALUE;
    Integer bestKey = null;
    for (Entry<Integer, Long> e : results.entrySet()) {
      Long value = e.getValue();
      if (value < lowestTime) {
        lowestTime = value;
        bestKey = e.getKey();
      }
    }
    return bestKey;
  }

  private static Map<Integer, Long> testWriteBuffer(PrintWriter out, String prefix, Random random,
      FileSystem fileSystem, Path path, long size, int min, int max) throws IOException {
    Map<Integer, Long> results = new TreeMap<Integer, Long>();
    for (int i = min; i <= max; i++) {
      int bufSize = (int) Math.pow(2, i);
      out.println(prefix + " Creating [" + size + "] length file for write testing using buffer size of [" + bufSize
          + "]");
      out.flush();
      FSDataOutputStream outputStream = fileSystem.create(new Path(path, "test.data"), true, bufSize);
      long time = writeFile(out, random, bufSize, outputStream, size);
      outputStream.close();
      results.put(bufSize, time);
      out.println(prefix + " Buffer size [" + bufSize + "] took [" + time / 1000000.0 + " ms]");
      out.flush();
    }
    return results;
  }

  private static Map<Integer, Long> testReadBuffer(PrintWriter out, String prefix, Random random,
      FileSystem fileSystem, Path path, long length, int min, int max, int readSamples) throws IOException {
    FSDataOutputStream outputStream = fileSystem.create(new Path(path, "test.data"), true, 8192);
    writeFile(out, random, 8192, outputStream, length);
    outputStream.close();

    Map<Integer, Long> results = new TreeMap<Integer, Long>();
    for (int i = min; i <= max; i++) {
      int bufSize = (int) Math.pow(2, i);
      out.println(prefix + " Reading [" + length + "] length file for reading test using buffer size of [" + bufSize
          + "]");
      out.flush();
      FSDataInputStream inputStream = fileSystem.open(new Path(path, "test.data"), bufSize);
      long time = readFile(out, random, bufSize, inputStream, length, readSamples);
      inputStream.close();
      results.put(bufSize, time);
      out.println(prefix + " Buffer size [" + bufSize + "] took [" + time / 1000000.0 + " ms]");
      out.flush();
    }
    return results;
  }

  private static long readFile(PrintWriter out, Random random, int bufSize, FSDataInputStream inputStream, long length,
      int readSamples) throws IOException {
    byte[] buf = new byte[bufSize];
    long start = System.nanoTime();
    long time = 0;
    for (int i = 0; i < readSamples; i++) {
      long now = System.nanoTime();
      if (start + 5000000000l < now) {
        double complete = (((double) i / (double) readSamples) * 100.0);
        out.println(complete + "% Complete");
        out.flush();
        start = System.nanoTime();
      }
      random.nextBytes(buf);
      long position = getPosition(bufSize, random, length);
      long s = System.nanoTime();
      int offset = 0;
      int len = bufSize;
      while (len > 0) {
        int amount = inputStream.read(position, buf, offset, len);
        len -= amount;
        offset += amount;
        position += amount;
      }
      long e = System.nanoTime();
      time += (e - s);
      length -= len;
    }
    return time;
  }

  private static long getPosition(int bufSize, Random random, long length) {
    return Math.abs(random.nextLong()) % (length - bufSize);
  }

  private static long writeFile(PrintWriter out, Random random, int bufSize, FSDataOutputStream outputStream,
      long length) throws IOException {
    byte[] buf = new byte[bufSize];
    final long origLength = length;
    long start = System.nanoTime();
    long time = 0;
    while (length > 0) {
      long now = System.nanoTime();
      if (start + 5000000000l < now) {
        double complete = ((origLength - length) / (double) origLength) * 100.0;
        out.println(complete + "% Complete");
        out.flush();
        start = System.nanoTime();
      }
      random.nextBytes(buf);
      int len = (int) Math.min(length, bufSize);
      long s = System.nanoTime();
      outputStream.write(buf, 0, len);
      long e = System.nanoTime();
      time += (e - s);
      length -= len;
    }
    return time;
  }
}
