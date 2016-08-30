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
package org.apache.blur.mapreduce.lib;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.LuceneVersionConstant;
import org.apache.blur.lucene.codec.Blur024Codec;
import org.apache.blur.mapreduce.lib.BlurMutate.MUTATE_TYPE;
import org.apache.blur.server.TableContext;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.RowDocumentUtil;
import org.apache.blur.utils.ShardUtil;
import org.apache.blur.utils.ThreadValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;

public class GenericBlurRecordWriter {

  private static final Log LOG = LogFactory.getLog(GenericBlurRecordWriter.class);
  private static final String JAVA_IO_TMPDIR = "java.io.tmpdir";
  private static final Counter NULL_COUNTER = NullCounter.getNullCounter();

  private final Text _prevKey = new Text();

  private final IndexWriter _writer;
  private final FieldManager _fieldManager;
  private final Directory _finalDir;
  private final Directory _localDir;
  private final File _localPath;
  // private final int _maxDocumentBufferSize;
  private final IndexWriterConfig _conf;
  private final IndexWriterConfig _overFlowConf;
  private final Path _newIndex;
  private final boolean _indexLocally;
  private final boolean _optimizeInFlight;
  private final DocumentBufferStrategy _documentBufferStrategy;
  private Counter _columnCount = NULL_COUNTER;
  private Counter _fieldCount = NULL_COUNTER;
  private Counter _recordCount = NULL_COUNTER;
  private Counter _rowCount = NULL_COUNTER;
  private Counter _recordDuplicateCount = NULL_COUNTER;
  private Counter _rowOverFlowCount = NULL_COUNTER;
  private Counter _rowDeleteCount = NULL_COUNTER;
  private RateCounter _recordRateCounter = new RateCounter(NULL_COUNTER);
  private RateCounter _rowRateCounter = new RateCounter(NULL_COUNTER);
  private RateCounter _copyRateCounter = new RateCounter(NULL_COUNTER);
  private boolean _countersSetup = false;
  private IndexWriter _localTmpWriter;
  private boolean _usingLocalTmpindex;
  private File _localTmpPath;
  private ProgressableDirectory _localTmpDir;
  private String _deletedRowId;
  private Configuration _configuration;
  private String _currentRowId;

  public GenericBlurRecordWriter(Configuration configuration, int attemptId, String tmpDirName) throws IOException {
    _configuration = configuration;
    _documentBufferStrategy = BlurOutputFormat.getDocumentBufferStrategy(_configuration);
    _indexLocally = BlurOutputFormat.isIndexLocally(_configuration);
    _optimizeInFlight = BlurOutputFormat.isOptimizeInFlight(_configuration);

    TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(_configuration);
    int shardCount = tableDescriptor.getShardCount();
    int shardId = attemptId % shardCount;

    Path tableOutput = BlurOutputFormat.getOutputPath(_configuration);
    String shardName = ShardUtil.getShardName(BlurConstants.SHARD_PREFIX, shardId);
    Path indexPath = new Path(tableOutput, shardName);
    _newIndex = new Path(indexPath, tmpDirName);
    _finalDir = new ProgressableDirectory(new HdfsDirectory(_configuration, _newIndex), getProgressable());
    _finalDir.setLockFactory(NoLockFactory.getNoLockFactory());

    TableContext tableContext = TableContext.create(tableDescriptor);
    _fieldManager = tableContext.getFieldManager();
    Analyzer analyzer = _fieldManager.getAnalyzerForIndex();

    _conf = new IndexWriterConfig(LuceneVersionConstant.LUCENE_VERSION, analyzer);
    _conf.setCodec(new Blur024Codec());
    _conf.setSimilarity(tableContext.getSimilarity());
    TieredMergePolicy mergePolicy = (TieredMergePolicy) _conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);

    _overFlowConf = _conf.clone();

    if (_indexLocally) {
      String localDirPath = System.getProperty(JAVA_IO_TMPDIR);
      _localPath = new File(localDirPath, UUID.randomUUID().toString() + ".tmp");
      SimpleFSDirectory directory = new SimpleFSDirectory(_localPath);
      _localDir = new ProgressableDirectory(directory, getProgressable());
      _writer = new IndexWriter(_localDir, _conf.clone());
    } else {
      _localPath = null;
      _localDir = null;
      _writer = new IndexWriter(_finalDir, _conf.clone());
    }
  }

  private Progressable getProgressable() {
    final Progressable prg = BlurOutputFormat.getProgressable();
    return new Progressable() {

      private Progressable _progressable = prg;
      private long _lastWarn = 0;
      private boolean _progressSetupLogged = false;

      @Override
      public void progress() {
        if (_progressable != null) {
          _progressable.progress();
          if (!_progressSetupLogged) {
            LOG.info("Progress has been setup correctly.");
            _progressSetupLogged = true;
          }
        } else {
          Progressable progressable = BlurOutputFormat.getProgressable();
          if (progressable != null) {
            _progressable = progressable;
          } else {
            long now = System.nanoTime();
            if (_lastWarn + TimeUnit.SECONDS.toNanos(10) < now) {
              LOG.warn("Progress not being reported.");
              _lastWarn = System.nanoTime();
            }
          }
        }
      }
    };
  }

  public void write(Text key, BlurMutate value) throws IOException {
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
    if (getCounter != null) {
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
  }

  private void add(BlurMutate value) throws IOException {
    BlurRecord blurRecord = value.getRecord();
    _currentRowId = blurRecord.getRowId();
    Record record = getRecord(blurRecord);
    String recordId = record.getRecordId();
    if (value.getMutateType() == MUTATE_TYPE.DELETE) {
      _deletedRowId = blurRecord.getRowId();
      return;
    }
    if (_countersSetup) {
      _columnCount.increment(record.getColumns().size());
    }
    List<Field> document = RowDocumentUtil.getDoc(_fieldManager, blurRecord.getRowId(), record);
    List<Field> dup = _documentBufferStrategy.add(recordId, document);
    if (_countersSetup) {
      if (dup != null) {
        _recordDuplicateCount.increment(1);
      } else {
        _fieldCount.increment(document.size());
        _recordCount.increment(1);
      }
    }
    flushToTmpIndexIfNeeded();
  }

  private void flushToTmpIndexIfNeeded() throws IOException {
    if (_documentBufferStrategy.isFull()) {
      LOG.info("RowId [" + _currentRowId + "] - Document Buffer is full overflow to disk.");
      flushToTmpIndex();
    }
  }

  private void flushToTmpIndex() throws IOException {
    if (_documentBufferStrategy.isEmpty()) {
      return;
    }
    _usingLocalTmpindex = true;
    if (_localTmpWriter == null) {
      String localDirPath = System.getProperty(JAVA_IO_TMPDIR);
      _localTmpPath = new File(localDirPath, UUID.randomUUID().toString() + ".tmp");
      SimpleFSDirectory directory = new SimpleFSDirectory(_localTmpPath);
      _localTmpDir = new ProgressableDirectory(directory, getProgressable());
      _localTmpWriter = new IndexWriter(_localTmpDir, _overFlowConf.clone());
      // The local tmp writer has merging disabled so the first document in is
      // going to be doc 0.
      // Therefore the first document added is the prime doc
      List<List<Field>> docs = _documentBufferStrategy.getAndClearBuffer();
      for (List<Field> doc : docs) {
        _localTmpWriter.addDocument(doc);
      }
    } else {
      List<List<Field>> docs = _documentBufferStrategy.getAndClearBuffer();
      for (List<Field> doc : docs) {
        _localTmpWriter.addDocument(doc);
      }
    }
  }

  private void resetLocalTmp() {
    _usingLocalTmpindex = false;
    _localTmpWriter = null;
    _localTmpDir = null;
    rm(_localTmpPath);
    _localTmpPath = null;
  }

  public static Record getRecord(BlurRecord value) {
    Record record = new Record();
    record.setRecordId(value.getRecordId());
    record.setFamily(value.getFamily());
    for (BlurColumn col : value.getColumns()) {
      record.addToColumns(new Column(col.getName(), col.getValue()));
    }
    return record;
  }

  private static ThreadValue<AtomicBoolean> _existingRow = new ThreadValue<AtomicBoolean>() {
    @Override
    protected AtomicBoolean initialValue() {
      return new AtomicBoolean();
    }
  };

  public static boolean isCurrentRowExisting() {
    return _existingRow.get().get();
  }

  public static void setCurrentRowExistingRowId(boolean existing) {
    _existingRow.get().set(existing);
  }

  private void flush() throws CorruptIndexException, IOException {
    boolean newRow = !isCurrentRowExisting();
    if (_usingLocalTmpindex) {
      // since we have flushed to disk then we do not need to index the
      // delete.
      flushToTmpIndex();
      LOG.info("RowId [" + _currentRowId + "] - forceMerge");
      _localTmpWriter.forceMerge(1, true);
      _localTmpWriter.close(true);

      DirectoryReader reader = DirectoryReader.open(_localTmpDir);
      AtomicReader atomicReader = getAtomicReader(reader);
      LOG.info("RowId [" + _currentRowId + "] - total documents [" + atomicReader.maxDoc() + "]");
      AtomicReader primeDocAtomicReader = PrimeDocOverFlowHelper.addPrimeDoc(atomicReader, newRow, _currentRowId);
      if (_countersSetup) {
        _recordRateCounter.mark(reader.numDocs());
      }
      _writer.addIndexes(primeDocAtomicReader);
      primeDocAtomicReader.close();
      resetLocalTmp();
      _writer.maybeMerge();
      LOG.info("RowId [" + _currentRowId + "] - add complete");
      if (_countersSetup) {
        _rowOverFlowCount.increment(1);
      }
    } else {
      if (_documentBufferStrategy.isEmpty()) {
        if (_deletedRowId != null) {
          _writer.addDocument(getDeleteDoc());
          if (_countersSetup) {
            _rowDeleteCount.increment(1);
          }
        }
      } else {
        List<List<Field>> docs = _documentBufferStrategy.getAndClearBuffer();
        docs.get(0).add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
        if (newRow) {
          docs.get(0).add(new StringField(BlurConstants.NEW_ROW, BlurConstants.PRIME_DOC_VALUE, Store.NO));
        } else {
          docs.get(0).add(new StringField(BlurConstants.UPDATE_ROW, _currentRowId, Store.NO));
        }
        _writer.addDocuments(docs);
        if (_countersSetup) {
          _recordRateCounter.mark(docs.size());
        }
      }
    }
    _deletedRowId = null;
    if (_countersSetup) {
      _rowRateCounter.mark();
      _rowCount.increment(1);
    }
  }

  private AtomicReader getAtomicReader(DirectoryReader reader) throws IOException {
    List<AtomicReaderContext> leaves = reader.leaves();
    if (leaves.size() == 1) {
      return leaves.get(0).reader();
    }
    throw new IOException("Reader [" + reader + "] has more than one segment after optimize.");
  }

  private Document getDeleteDoc() {
    Document document = new Document();
    document.add(new StringField(BlurConstants.ROW_ID, _deletedRowId, Store.NO));
    document.add(new StringField(BlurConstants.DELETE_MARKER, BlurConstants.DELETE_MARKER_VALUE, Store.NO));
    document.add(new StringField(BlurConstants.UPDATE_ROW, _deletedRowId, Store.NO));
    return document;
  }

  public void close() throws IOException {
    flush();
    _writer.close(false);
    if (_countersSetup) {
      _recordRateCounter.close();
      _rowRateCounter.close();
    }
    if (_indexLocally) {
      if (_optimizeInFlight) {
        copyAndOptimizeInFlightDir();
      } else {
        copyDir();
      }
    }
    if (_countersSetup) {
      _copyRateCounter.close();
    }
  }

  private void copyAndOptimizeInFlightDir() throws IOException {
    CopyRateDirectory copyRateDirectory = new CopyRateDirectory(_finalDir, _copyRateCounter);
    copyRateDirectory.setLockFactory(NoLockFactory.getNoLockFactory());
    DirectoryReader reader = DirectoryReader.open(_localDir);
    IndexWriter writer = new IndexWriter(copyRateDirectory, _conf.clone());
    writer.addIndexes(reader);
    writer.setCommitData(getInternalMarker());
    writer.close();
    rm(_localPath);
  }

  private Map<String, String> getInternalMarker() {
    Map<String, String> map = new HashMap<String, String>();
    map.put(BlurConstants.INTERNAL, BlurConstants.INTERNAL);
    return map;
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
