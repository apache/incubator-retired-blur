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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.LuceneVersionConstant;
import org.apache.blur.manager.writer.TransactionRecorder;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NoLockFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

public class BlurOutputFormat extends OutputFormat<Text, BlurMutate> {

  public static final String BLUR_TABLE_DESCRIPTOR = "blur.table.descriptor";
  public static final String BLUR_OUTPUT_PATH = "blur.output.path";

  private static final String JAVA_IO_TMPDIR = "java.io.tmpdir";
  private static final String MAPRED_OUTPUT_COMMITTER_CLASS = "mapred.output.committer.class";
  private static ThreadLocal<Progressable> _progressable = new ThreadLocal<Progressable>();
  private static ThreadLocal<GetCounter> _getCounter = new ThreadLocal<GetCounter>();

  static void setProgressable(Progressable progressable) {
    _progressable.set(progressable);
  }

  static Progressable getProgressable() {
    return _progressable.get();
  }

  static void setGetCounter(GetCounter getCounter) {
    _getCounter.set(getCounter);
  }

  static GetCounter getGetCounter() {
    return _getCounter.get();
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

  }

  @Override
  public RecordWriter<Text, BlurMutate> getRecordWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {
    int id = context.getTaskAttemptID().getTaskID().getId();
    TaskAttemptID taskAttemptID = context.getTaskAttemptID();
    return new BlurRecordWriter(context.getConfiguration(), new BlurAnalyzer(), id, taskAttemptID.toString() + ".tmp");
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    int numReduceTasks = context.getNumReduceTasks();
    if (numReduceTasks != 0) {
      try {
        Class<? extends OutputFormat<?, ?>> outputFormatClass = context.getOutputFormatClass();
        if (outputFormatClass.equals(BlurOutputFormat.class)) {
          // Then only reducer needs committer.
          if (context.getTaskAttemptID().isMap()) {
            return getDoNothing();
          }
        }
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
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

  public static void setTableDescriptor(Job job, TableDescriptor tableDescriptor) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    TIOStreamTransport transport = new TIOStreamTransport(outputStream);
    TJSONProtocol protocol = new TJSONProtocol(transport);
    try {
      tableDescriptor.write(protocol);
    } catch (TException e) {
      throw new IOException(e);
    }
    transport.close();
    Configuration configuration = job.getConfiguration();
    configuration.set(BLUR_TABLE_DESCRIPTOR, new String(outputStream.toByteArray()));
    setOutputPath(job, new Path(tableDescriptor.getTableUri()));
  }

  public static void setOutputPath(Job job, Path path) {
    Configuration configuration = job.getConfiguration();
    configuration.set(BLUR_OUTPUT_PATH, path.toString());
    configuration.set(MAPRED_OUTPUT_COMMITTER_CLASS, BlurOutputCommitter.class.getName());
  }

  public static Path getOutputPath(Configuration configuration) {
    return new Path(configuration.get(BLUR_OUTPUT_PATH));
  }

  static class BlurRecordWriter extends RecordWriter<Text, BlurMutate> {

    private static final Log LOG = LogFactory.getLog(BlurRecordWriter.class);

    private final Text _prevKey = new Text();
    private final List<Document> _documents = new ArrayList<Document>();
    private final IndexWriter _writer;
    private final BlurAnalyzer _analyzer;
    private final StringBuilder _builder = new StringBuilder();
    private final Directory _finalDir;
    private final Directory _localDir;
    private final File _localPath;
    private Counter _fieldCount;
    private Counter _recordCount;
    private Counter _rowCount;
    private boolean _countersSetup = false;
    private RateCounter _recordRateCounter;
    private RateCounter _rowRateCounter;
    private RateCounter _copyRateCounter;

    public BlurRecordWriter(Configuration configuration, BlurAnalyzer blurAnalyzer, int shardId, String tmpDirName)
        throws IOException {
      Path tableOutput = BlurOutputFormat.getOutputPath(configuration);
      String shardName = BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, shardId);
      Path indexPath = new Path(tableOutput, shardName);
      Path newIndex = new Path(indexPath, tmpDirName);
      _finalDir = new ProgressableDirectory(new HdfsDirectory(configuration, newIndex),
          BlurOutputFormat.getProgressable());
      _finalDir.setLockFactory(NoLockFactory.getNoLockFactory());

      TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(configuration);
      _analyzer = new BlurAnalyzer(tableDescriptor.getAnalyzerDefinition());
      IndexWriterConfig conf = new IndexWriterConfig(LuceneVersionConstant.LUCENE_VERSION, _analyzer);

      String localDirPath = System.getProperty(JAVA_IO_TMPDIR);
      _localPath = new File(localDirPath, UUID.randomUUID().toString() + ".tmp");
      _localDir = new ProgressableDirectory(FSDirectory.open(_localPath), BlurOutputFormat.getProgressable());
      _writer = new IndexWriter(_localDir, conf);
    }

    @Override
    public void write(Text key, BlurMutate value) throws IOException, InterruptedException {
      if (!_countersSetup) {
        setupCounter();
        _countersSetup = true;
      }
      if (!_prevKey.equals(key)) {
        flush();
        _prevKey.set(key);
      }
      add(value);
    }

    private void setupCounter() {
      GetCounter getCounter = BlurOutputFormat.getGetCounter();
      _fieldCount = getCounter.getCounter(BlurCounters.FIELD_COUNT);
      _recordCount = getCounter.getCounter(BlurCounters.RECORD_COUNT);
      _rowCount = getCounter.getCounter(BlurCounters.ROW_COUNT);
      _recordRateCounter = new RateCounter(getCounter.getCounter(BlurCounters.RECORD_RATE));
      _rowRateCounter = new RateCounter(getCounter.getCounter(BlurCounters.ROW_RATE));
      _copyRateCounter = new RateCounter(getCounter.getCounter(BlurCounters.COPY_RATE));
    }

    private void add(BlurMutate value) {
      BlurRecord blurRecord = value.getRecord();
      Record record = getRecord(blurRecord);
      Document document = TransactionRecorder.convert(blurRecord.getRowId(), record, _builder, _analyzer);
      if (_documents.size() == 0) {
        document.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
      }
      _documents.add(document);
      _fieldCount.increment(document.getFields().size());
      _recordCount.increment(1);
    }

    private Record getRecord(BlurRecord value) {
      Record record = new Record();
      record.setRecordId(value.getRecordId());
      record.setFamily(value.getFamily());
      for (BlurColumn col : value.getColumns()) {
        record.addToColumns(new Column(col.getName(), col.getValue()));
      }
      return record;
    }

    private void flush() throws CorruptIndexException, IOException {
      if (_documents.isEmpty()) {
        return;
      }
      _writer.addDocuments(_documents);
      _rowCount.increment(1);
      _recordRateCounter.mark(_documents.size());
      _rowRateCounter.mark();
      _documents.clear();
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      flush();
      _writer.close();
      _recordRateCounter.close();
      _rowRateCounter.close();
      copyDir();
      _copyRateCounter.close();
    }

    private void copyDir() throws IOException {
      CopyRateDirectory copyRateDirectory = new CopyRateDirectory(_finalDir, _copyRateCounter);
      String[] fileNames = _localDir.listAll();
      for (String fileName : fileNames) {
        LOG.info("Copying [{0}]", fileName);
        _localDir.copy(copyRateDirectory, fileName, fileName, IOContext.DEFAULT);
      }
      rm(_localPath);
    }

    private void rm(File file) {
      if (!file.exists()) {
        return;
      }
      if (file.isDirectory()) {
        for (File f : file.listFiles()) {
          rm(f);
        }
      }
      file.delete();
    }
  }

  /**
   * Sets up the output postion of the map reduce job. This does effect the map
   * side of the job.
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
  }

  private OutputCommitter getDoNothing() {
    return new OutputCommitter() {

      @Override
      public void commitJob(JobContext jobContext) throws IOException {
      }

      @Override
      public void cleanupJob(JobContext context) throws IOException {
      }

      @Override
      public void abortJob(JobContext jobContext, State state) throws IOException {
      }

      @Override
      public void setupTask(TaskAttemptContext taskContext) throws IOException {

      }

      @Override
      public void setupJob(JobContext jobContext) throws IOException {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskContext) throws IOException {

      }

      @Override
      public void abortTask(TaskAttemptContext taskContext) throws IOException {

      }
    };
  }

}
