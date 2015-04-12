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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TJSONProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TIOStreamTransport;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;

/**
 * {@link BlurOutputFormat} is used to index data and delivery the indexes to
 * the proper Blur table for searching. A typical usage of this class would be
 * as follows.<br/>
 * <br/>
 * 
 * <br/>
 * {@link Iface} client = {@link BlurClient}.getClient("controller1:40010");<br/>
 * <br/>
 * TableDescriptor tableDescriptor = client.describe(tableName);<br/>
 * <br/>
 * Job job = new Job(jobConf, "blur index");<br/>
 * job.setJarByClass(BlurOutputFormatTest.class);<br/>
 * job.setMapperClass(CsvBlurMapper.class);<br/>
 * job.setInputFormatClass(TextInputFormat.class);<br/>
 * <br/>
 * FileInputFormat.addInputPath(job, new Path(input));<br/>
 * CsvBlurMapper.addColumns(job, "cf1", "col");<br/>
 * <br/>
 * BlurOutputFormat.setupJob(job, tableDescriptor);<br/>
 * BlurOutputFormat.setIndexLocally(job, true);<br/>
 * BlurOutputFormat.setOptimizeInFlight(job, false);<br/>
 * <br/>
 * job.waitForCompletion(true);<br/>
 * 
 */
public class BlurOutputFormat extends OutputFormat<Text, BlurMutate> {

  public static final String BLUR_OUTPUT_REDUCER_MULTIPLIER = "blur.output.reducer.multiplier";
  public static final String BLUR_OUTPUT_OPTIMIZEINFLIGHT = "blur.output.optimizeinflight";
  public static final String BLUR_OUTPUT_INDEXLOCALLY = "blur.output.indexlocally";
  public static final String BLUR_OUTPUT_MAX_DOCUMENT_BUFFER_SIZE = "blur.output.max.document.buffer.size";
  public static final String BLUR_OUTPUT_MAX_DOCUMENT_BUFFER_HEAP_SIZE = "blur.output.max.document.buffer.heap.size";
  public static final String BLUR_OUTPUT_DOCUMENT_BUFFER_STRATEGY = "blur.output.document.buffer.strategy";
  public static final String BLUR_TABLE_DESCRIPTOR = "blur.table.descriptor";
  public static final String BLUR_OUTPUT_PATH = "blur.output.path";

  private static final String MAPRED_OUTPUT_COMMITTER_CLASS = "mapred.output.committer.class";
  private static ThreadLocal<Progressable> _progressable = new ThreadLocal<Progressable>();
  private static ThreadLocal<GetCounter> _getCounter = new ThreadLocal<GetCounter>();

  public static void setProgressable(Progressable progressable) {
    _progressable.set(progressable);
  }

  public static Progressable getProgressable() {
    return _progressable.get();
  }

  public static void setGetCounter(GetCounter getCounter) {
    _getCounter.set(getCounter);
  }

  public static GetCounter getGetCounter() {
    return _getCounter.get();
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    CheckOutputSpecs.checkOutputSpecs(context.getConfiguration(), context.getNumReduceTasks());
  }

  @Override
  public RecordWriter<Text, BlurMutate> getRecordWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {
    int id = context.getTaskAttemptID().getTaskID().getId();
    TaskAttemptID taskAttemptID = context.getTaskAttemptID();
    final GenericBlurRecordWriter writer = new GenericBlurRecordWriter(context.getConfiguration(), id,
        taskAttemptID.toString() + ".tmp");
    return new RecordWriter<Text, BlurMutate>() {

      @Override
      public void write(Text key, BlurMutate value) throws IOException, InterruptedException {
        writer.write(key, value);
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        writer.close();
      }
    };
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new BlurOutputCommitter();
  }

  public static TableDescriptor getTableDescriptor(Configuration configuration) throws IOException {
    String tableDesStr = configuration.get(BLUR_TABLE_DESCRIPTOR);
    if (tableDesStr == null) {
      return null;
    }
    ByteArrayInputStream inputStream = new ByteArrayInputStream(tableDesStr.getBytes());
    TIOStreamTransport transport = new TIOStreamTransport(inputStream);
    TJSONProtocol protocol = new TJSONProtocol(transport);
    TableDescriptor descriptor = new TableDescriptor();
    try {
      descriptor.read(protocol);
    } catch (TException e) {
      throw new IOException(e);
    }
    transport.close();
    return descriptor;
  }

  /**
   * This will multiple the number of reducers for this job. For example if the
   * table has 256 shards the normal number of reducers is 256. However if the
   * reducer multiplier is set to 4 then the number of reducers will be 1024 and
   * each shard will get 4 new segments instead of the normal 1.
   * 
   * @param job
   *          the job to setup.
   * @param multiple
   *          the multiple to use.
   * @throws IOException
   */
  public static void setReducerMultiplier(Job job, int multiple) throws IOException {
    TableDescriptor tableDescriptor = getTableDescriptor(job.getConfiguration());
    if (tableDescriptor == null) {
      throw new IOException("setTableDescriptor needs to be called first.");
    }
    job.setNumReduceTasks(tableDescriptor.getShardCount() * multiple);
    Configuration configuration = job.getConfiguration();
    configuration.setInt(BLUR_OUTPUT_REDUCER_MULTIPLIER, multiple);
  }

  public static int getReducerMultiplier(Configuration configuration) {
    return configuration.getInt(BLUR_OUTPUT_REDUCER_MULTIPLIER, 1);
  }

  /**
   * Sets the {@link TableDescriptor} for this job.
   * 
   * @param job
   *          the job to setup.
   * @param tableDescriptor
   *          the {@link TableDescriptor}.
   * @throws IOException
   */
  public static void setTableDescriptor(Job job, TableDescriptor tableDescriptor) throws IOException {
    setTableDescriptor(job.getConfiguration(), tableDescriptor);
  }

  /**
   * Sets the {@link TableDescriptor} for this job.
   * 
   * @param job
   *          the job to setup.
   * @param tableDescriptor
   *          the {@link TableDescriptor}.
   * @throws IOException
   */
  public static void setTableDescriptor(Configuration configuration, TableDescriptor tableDescriptor)
      throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    TIOStreamTransport transport = new TIOStreamTransport(outputStream);
    TJSONProtocol protocol = new TJSONProtocol(transport);
    try {
      tableDescriptor.write(protocol);
    } catch (TException e) {
      throw new IOException(e);
    }
    transport.close();
    configuration.set(BLUR_TABLE_DESCRIPTOR, new String(outputStream.toByteArray()));
  }

  /**
   * Sets the maximum number of documents that the buffer will hold in memory
   * before overflowing to disk. By default this is 1000 which will probably be
   * very low for most systems.
   * 
   * @param job
   *          the job to setup.
   * @param maxDocumentBufferSize
   *          the maxDocumentBufferSize.
   */
  public static void setMaxDocumentBufferSize(Job job, int maxDocumentBufferSize) {
    setMaxDocumentBufferSize(job.getConfiguration(), maxDocumentBufferSize);
  }

  /**
   * Sets the maximum number of documents that the buffer will hold in memory
   * before overflowing to disk. By default this is 1000 which will probably be
   * very low for most systems.
   * 
   * @param configuration
   *          the configuration to setup.
   * @param maxDocumentBufferSize
   *          the maxDocumentBufferSize.
   */
  public static void setMaxDocumentBufferSize(Configuration configuration, int maxDocumentBufferSize) {
    configuration.setInt(BLUR_OUTPUT_MAX_DOCUMENT_BUFFER_SIZE, maxDocumentBufferSize);
  }

  public static int getMaxDocumentBufferSize(Configuration configuration) {
    return configuration.getInt(BLUR_OUTPUT_MAX_DOCUMENT_BUFFER_SIZE, 1000);
  }

  public static int getMaxDocumentBufferHeapSize(Configuration configuration) {
    return configuration.getInt(BLUR_OUTPUT_MAX_DOCUMENT_BUFFER_HEAP_SIZE, 32 * 1024 * 1024);
  }

  public static void setMaxDocumentBufferHeapSize(Configuration configuration, int maxDocumentBufferHeapSize) {
    configuration.setInt(BLUR_OUTPUT_MAX_DOCUMENT_BUFFER_HEAP_SIZE, maxDocumentBufferHeapSize);
  }

  public static void setMaxDocumentBufferHeapSize(Job job, int maxDocumentBufferHeapSize) {
    setMaxDocumentBufferHeapSize(job.getConfiguration(), maxDocumentBufferHeapSize);
  }

  public static DocumentBufferStrategy getDocumentBufferStrategy(Configuration configuration) {
    Class<? extends DocumentBufferStrategy> clazz = configuration.getClass(BLUR_OUTPUT_DOCUMENT_BUFFER_STRATEGY, DocumentBufferStrategyFixedSize.class, DocumentBufferStrategy.class);
    try {
      Constructor<? extends DocumentBufferStrategy> constructor = clazz.getConstructor(new Class[]{Configuration.class});
      return constructor.newInstance(new Object[]{configuration});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public static void setDocumentBufferStrategy(Job job, Class<? extends DocumentBufferStrategy> documentBufferStrategyClass) {
    setDocumentBufferStrategy(job.getConfiguration(), documentBufferStrategyClass);
  }
  
  public static void setDocumentBufferStrategy(Configuration configuration, Class<? extends DocumentBufferStrategy> documentBufferStrategyClass) {
    configuration.setClass(BLUR_OUTPUT_DOCUMENT_BUFFER_STRATEGY, documentBufferStrategyClass, DocumentBufferStrategy.class);
  }

  public static void setOutputPath(Job job, Path path) {
    setOutputPath(job.getConfiguration(), path);
  }

  public static void setOutputPath(Configuration configuration, Path path) {
    configuration.set(BLUR_OUTPUT_PATH, path.toString());
    configuration.set(MAPRED_OUTPUT_COMMITTER_CLASS, BlurOutputCommitter.class.getName());
  }

  public static Path getOutputPath(Configuration configuration) {
    String pathString = configuration.get(BLUR_OUTPUT_PATH);
    if (pathString == null) {
      return null;
    }
    return new Path(pathString);
  }

  /**
   * Enabled by default, this will enable local indexing on the machine where
   * the task is running. Then when the {@link RecordWriter} closes the index is
   * copied to the remote destination in HDFS.
   * 
   * @param job
   *          the job to setup.
   * @param b
   *          the boolean to true enable, false to disable.
   */
  public static void setIndexLocally(Job job, boolean b) {
    setIndexLocally(job.getConfiguration(), b);
  }

  /**
   * Enabled by default, this will enable local indexing on the machine where
   * the task is running. Then when the {@link RecordWriter} closes the index is
   * copied to the remote destination in HDFS.
   * 
   * @param configuration
   *          the configuration to setup.
   * @param b
   *          the boolean to true enable, false to disable.
   */
  public static void setIndexLocally(Configuration configuration, boolean b) {
    configuration.setBoolean(BLUR_OUTPUT_INDEXLOCALLY, b);
  }

  public static boolean isIndexLocally(Configuration configuration) {
    return configuration.getBoolean(BLUR_OUTPUT_INDEXLOCALLY, true);
  }

  /**
   * Enabled by default, this will optimize the index while copying from the
   * local index to the remote destination in HDFS. Used in conjunction with the
   * setIndexLocally.
   * 
   * @param job
   *          the job to setup.
   * @param b
   *          the boolean to true enable, false to disable.
   */
  public static void setOptimizeInFlight(Job job, boolean b) {
    setOptimizeInFlight(job.getConfiguration(), b);
  }

  /**
   * Enabled by default, this will optimize the index while copying from the
   * local index to the remote destination in HDFS. Used in conjunction with the
   * setIndexLocally.
   * 
   * @param job
   *          the job to setup.
   * @param b
   *          the boolean to true enable, false to disable.
   */
  public static void setOptimizeInFlight(Configuration configuration, boolean b) {
    configuration.setBoolean(BLUR_OUTPUT_OPTIMIZEINFLIGHT, b);
  }

  public static boolean isOptimizeInFlight(Configuration configuration) {
    return configuration.getBoolean(BLUR_OUTPUT_OPTIMIZEINFLIGHT, true);
  }

  /**
   * Sets up the output portion of the map reduce job. This does effect the map
   * side of the job, of a map and reduce job.
   * 
   * @param job
   *          the job to setup.
   * @param tableDescriptor
   *          the table descriptor to write the output of the indexing job.
   * @throws IOException
   */
  public static void setupJob(Job job, TableDescriptor tableDescriptor) throws IOException {
    job.setReducerClass(DefaultBlurReducer.class);
    job.setNumReduceTasks(tableDescriptor.getShardCount());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BlurMutate.class);
    job.setOutputFormatClass(BlurOutputFormat.class);
    setTableDescriptor(job, tableDescriptor);
    BlurMapReduceUtil.addDependencyJars(job);
    BlurMapReduceUtil.addAllJarsInBlurLib(job.getConfiguration());
  }

}
