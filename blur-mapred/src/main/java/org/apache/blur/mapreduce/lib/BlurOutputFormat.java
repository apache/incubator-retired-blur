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
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.LuceneVersionConstant;
import org.apache.blur.manager.writer.TransactionRecorder;
import org.apache.blur.mapreduce.lib.BlurMutate.MUTATE_TYPE;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TJSONProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TIOStreamTransport;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NoLockFactory;

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
    Configuration config = context.getConfiguration();
    TableDescriptor tableDescriptor = getTableDescriptor(config);
    if (tableDescriptor == null) {
      throw new IOException("setTableDescriptor needs to be called first.");
    }
    int shardCount = tableDescriptor.getShardCount();
    FileSystem fileSystem = getOutputPath(config).getFileSystem(config);
    Path tablePath = new Path(tableDescriptor.getTableUri());
    if (fileSystem.exists(tablePath)) {
      BlurUtil.validateShardCount(shardCount, fileSystem, tablePath);
    } else {
      throw new IOException("Table path [ " + tablePath + " ] doesn't exist for table [ " + tableDescriptor.getName()
          + " ].");
    }
    BlurUtil.validateWritableDirectory(fileSystem, tablePath);
    int reducers = context.getNumReduceTasks();
    int reducerMultiplier = getReducerMultiplier(config);
    int validNumberOfReducers = reducerMultiplier * shardCount;
    if (reducers > 0 && reducers != validNumberOfReducers) {
      throw new IllegalArgumentException("Invalid number of reducers [ " + reducers + " ]."
          + " Number of Reducers should be [ " + validNumberOfReducers + " ].");
    }
  }

  @Override
  public RecordWriter<Text, BlurMutate> getRecordWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {
    int id = context.getTaskAttemptID().getTaskID().getId();
    TaskAttemptID taskAttemptID = context.getTaskAttemptID();
    return new BlurRecordWriter(context.getConfiguration(), id, taskAttemptID.toString() + ".tmp");
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new BlurOutputCommitter(context.getTaskAttemptID().isMap(), context.getNumReduceTasks());
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

  public static void setOutputPath(Job job, Path path) {
    Configuration configuration = job.getConfiguration();
    configuration.set(BLUR_OUTPUT_PATH, path.toString());
    configuration.set(MAPRED_OUTPUT_COMMITTER_CLASS, BlurOutputCommitter.class.getName());
  }

  public static Path getOutputPath(Configuration configuration) {
    return new Path(configuration.get(BLUR_OUTPUT_PATH));
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

  static class BlurRecordWriter extends RecordWriter<Text, BlurMutate> {

    private static final Log LOG = LogFactory.getLog(BlurRecordWriter.class);

    private final Text _prevKey = new Text();
    private final Map<String, Document> _documents = new TreeMap<String, Document>();
    private final IndexWriter _writer;
    private final BlurAnalyzer _analyzer;
    private final Directory _finalDir;
    private final Directory _localDir;
    private final File _localPath;
    private final int _maxDocumentBufferSize;
    private final IndexWriterConfig _conf;
    private final Path _newIndex;
    private final boolean _indexLocally;
    private final boolean _optimizeInFlight;
    private Counter _columnCount = emptyCounter();
    private Counter _fieldCount = emptyCounter();
    private Counter _recordCount = emptyCounter();
    private Counter _rowCount = emptyCounter();
    private Counter _recordDuplicateCount = emptyCounter();
    private Counter _rowOverFlowCount = emptyCounter();
    private Counter _rowDeleteCount = emptyCounter();
    private RateCounter _recordRateCounter = new RateCounter(emptyCounter());
    private RateCounter _rowRateCounter = new RateCounter(emptyCounter());
    private RateCounter _copyRateCounter = new RateCounter(emptyCounter());
    private boolean _countersSetup = false;
    private IndexWriter _localTmpWriter;
    private boolean _usingLocalTmpindex;
    private File _localTmpPath;
    private ProgressableDirectory _localTmpDir;
    private String _deletedRowId;
    

    public BlurRecordWriter(Configuration configuration, int attemptId, String tmpDirName)
        throws IOException {

      _indexLocally = BlurOutputFormat.isIndexLocally(configuration);
      _optimizeInFlight = BlurOutputFormat.isOptimizeInFlight(configuration);

      TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(configuration);
      int shardCount = tableDescriptor.getShardCount();
      int shardId = attemptId % shardCount;

      _maxDocumentBufferSize = BlurOutputFormat.getMaxDocumentBufferSize(configuration);
      Path tableOutput = BlurOutputFormat.getOutputPath(configuration);
      String shardName = BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, shardId);
      Path indexPath = new Path(tableOutput, shardName);
      _newIndex = new Path(indexPath, tmpDirName);
      _finalDir = new ProgressableDirectory(new HdfsDirectory(configuration, _newIndex),
          BlurOutputFormat.getProgressable());
      _finalDir.setLockFactory(NoLockFactory.getNoLockFactory());

      _analyzer = new BlurAnalyzer(tableDescriptor.getAnalyzerDefinition());
      _conf = new IndexWriterConfig(LuceneVersionConstant.LUCENE_VERSION, _analyzer);
      TieredMergePolicy mergePolicy = (TieredMergePolicy) _conf.getMergePolicy();
      mergePolicy.setUseCompoundFile(false);

      if (_indexLocally) {
        String localDirPath = System.getProperty(JAVA_IO_TMPDIR);
        _localPath = new File(localDirPath, UUID.randomUUID().toString() + ".tmp");
        _localDir = new ProgressableDirectory(FSDirectory.open(_localPath), BlurOutputFormat.getProgressable());
        _writer = new IndexWriter(_localDir, _conf.clone());
      } else {
        _localPath = null;
        _localDir = null;
        _writer = new IndexWriter(_finalDir, _conf.clone());
      }
    }

    private Counter emptyCounter() {
      return new Counter() {
      };
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
      _fieldCount = getCounter.getCounter(BlurCounters.LUCENE_FIELD_COUNT);
      _columnCount = getCounter.getCounter(BlurCounters.COLUMN_COUNT);
      _recordCount = getCounter.getCounter(BlurCounters.RECORD_COUNT);
      _recordDuplicateCount = getCounter.getCounter(BlurCounters.RECORD_DUPLICATE_COUNT);
      _rowCount = getCounter.getCounter(BlurCounters.ROW_COUNT);
      _rowDeleteCount = getCounter.getCounter(BlurCounters.ROW_DELETE_COUNT);
      _rowOverFlowCount = getCounter.getCounter(BlurCounters.ROW_OVERFLOW_COUNT);
      _recordRateCounter = new RateCounter(getCounter.getCounter(BlurCounters.RECORD_RATE));
      _rowRateCounter = new RateCounter(getCounter.getCounter(BlurCounters.ROW_RATE));
      _copyRateCounter = new RateCounter(getCounter.getCounter(BlurCounters.COPY_RATE));
    }

    private void add(BlurMutate value) throws IOException {
      BlurRecord blurRecord = value.getRecord();
      Record record = getRecord(blurRecord);
      String recordId = record.getRecordId();
      if (value.getMutateType() == MUTATE_TYPE.DELETE) {
        _deletedRowId = blurRecord.getRowId();
        return;
      }
      _columnCount.increment(record.getColumns().size());
      Document document = TransactionRecorder.convert(blurRecord.getRowId(), record, _analyzer);
      if (_documents.size() == 0) {
        document.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
      }
      Document dup = _documents.put(recordId, document);
      if (dup != null) {
        _recordDuplicateCount.increment(1);
      } else {
        _fieldCount.increment(document.getFields().size());
        _recordCount.increment(1);
      }
      flushToTmpIndexIfNeeded();
    }

    private void flushToTmpIndexIfNeeded() throws IOException {
      if (_documents.size() > _maxDocumentBufferSize) {
        flushToTmpIndex();
      }
    }

    private void flushToTmpIndex() throws IOException {
      if (_documents.isEmpty()) {
        return;
      }
      _usingLocalTmpindex = true;
      if (_localTmpWriter == null) {
        String localDirPath = System.getProperty(JAVA_IO_TMPDIR);
        _localTmpPath = new File(localDirPath, UUID.randomUUID().toString() + ".tmp");
        _localTmpDir = new ProgressableDirectory(FSDirectory.open(_localTmpPath), BlurOutputFormat.getProgressable());
        _localTmpWriter = new IndexWriter(_localTmpDir, _conf.clone());
      }
      _localTmpWriter.addDocuments(_documents.values());
      _documents.clear();
    }

    private void resetLocalTmp() {
      _usingLocalTmpindex = false;
      _localTmpWriter = null;
      _localTmpDir = null;
      rm(_localTmpPath);
      _localTmpPath = null;
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
      if (_usingLocalTmpindex) {
        // since we have flushed to disk then we do not need to index the
        // delete.
        flushToTmpIndex();
        _localTmpWriter.close(false);
        DirectoryReader reader = DirectoryReader.open(_localTmpDir);
        _recordRateCounter.mark(reader.numDocs());
        _writer.addIndexes(reader);
        reader.close();
        resetLocalTmp();
        _rowOverFlowCount.increment(1);
      } else {
        if (_documents.isEmpty()) {
          if (_deletedRowId != null) {
            _writer.addDocument(getDeleteDoc());
            _rowDeleteCount.increment(1);
          } else {
            LOG.info("This case should never happen, no records to index and no row deletes to emit.");
          }
        } else {
          _writer.addDocuments(_documents.values());
          _recordRateCounter.mark(_documents.size());
          _documents.clear();
        }
      }
      _deletedRowId = null;
      _rowRateCounter.mark();
      _rowCount.increment(1);
    }

    private Document getDeleteDoc() {
      Document document = new Document();
      document.add(new StringField(BlurConstants.ROW_ID, _deletedRowId, Store.NO));
      document.add(new StringField(BlurConstants.DELETE_MARKER, BlurConstants.DELETE_MARKER_VALUE, Store.NO));
      return document;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      flush();
      _writer.close();
      _recordRateCounter.close();
      _rowRateCounter.close();
      if (_indexLocally) {
        if (_optimizeInFlight) {
          copyAndOptimizeInFlightDir();
        } else {
          copyDir();
        }
      }
      _copyRateCounter.close();
    }

    private void copyAndOptimizeInFlightDir() throws IOException {
      CopyRateDirectory copyRateDirectory = new CopyRateDirectory(_finalDir, _copyRateCounter);
      copyRateDirectory.setLockFactory(NoLockFactory.getNoLockFactory());
      DirectoryReader reader = DirectoryReader.open(_localDir);
      IndexWriter writer = new IndexWriter(copyRateDirectory, _conf.clone());
      writer.addIndexes(reader);
      writer.close();
      rm(_localPath);
    }

    private void copyDir() throws IOException {
      CopyRateDirectory copyRateDirectory = new CopyRateDirectory(_finalDir, _copyRateCounter);
      String[] fileNames = _localDir.listAll();
      for (String fileName : fileNames) {
        LOG.info("Copying [{0}] to [{1}]", fileName, _newIndex);
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
  }

}
