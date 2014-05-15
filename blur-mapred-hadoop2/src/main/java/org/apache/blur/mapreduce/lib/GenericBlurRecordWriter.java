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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.LuceneVersionConstant;
import org.apache.blur.lucene.codec.Blur022Codec;
import org.apache.blur.mapreduce.lib.BlurMutate.MUTATE_TYPE;
import org.apache.blur.server.TableContext;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.RowDocumentUtil;
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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NoLockFactory;

public class GenericBlurRecordWriter {

  private static final Log LOG = LogFactory.getLog(GenericBlurRecordWriter.class);
  private static final String JAVA_IO_TMPDIR = "java.io.tmpdir";

  private final Text _prevKey = new Text();
  private final Map<String, List<Field>> _documents = new TreeMap<String, List<Field>>();
  private final IndexWriter _writer;
  private final FieldManager _fieldManager;
  private final Directory _finalDir;
  private final Directory _localDir;
  private final File _localPath;
  private final int _maxDocumentBufferSize;
  private final IndexWriterConfig _conf;
  private final IndexWriterConfig _overFlowConf;
  private final Path _newIndex;
  private final boolean _indexLocally;
  private final boolean _optimizeInFlight;
  private Counter _columnCount;
  private Counter _fieldCount;
  private Counter _recordCount;
  private Counter _rowCount;
  private Counter _recordDuplicateCount;
  private Counter _rowOverFlowCount;
  private Counter _rowDeleteCount;
  private RateCounter _recordRateCounter;
  private RateCounter _rowRateCounter;
  private RateCounter _copyRateCounter;
  private boolean _countersSetup = false;
  private IndexWriter _localTmpWriter;
  private boolean _usingLocalTmpindex;
  private File _localTmpPath;
  private ProgressableDirectory _localTmpDir;
  private String _deletedRowId;

  public GenericBlurRecordWriter(Configuration configuration, int attemptId, String tmpDirName) throws IOException {

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
    _finalDir = new ProgressableDirectory(new HdfsDirectory(configuration, _newIndex), getProgressable());
    _finalDir.setLockFactory(NoLockFactory.getNoLockFactory());

    TableContext tableContext = TableContext.create(tableDescriptor);
    _fieldManager = tableContext.getFieldManager();
    Analyzer analyzer = _fieldManager.getAnalyzerForIndex();

    _conf = new IndexWriterConfig(LuceneVersionConstant.LUCENE_VERSION, analyzer);
    _conf.setCodec(new Blur022Codec());
    _conf.setSimilarity(tableContext.getSimilarity());
    TieredMergePolicy mergePolicy = (TieredMergePolicy) _conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);

    _overFlowConf = _conf.clone();
    _overFlowConf.setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES);

    if (_indexLocally) {
      String localDirPath = System.getProperty(JAVA_IO_TMPDIR);
      _localPath = new File(localDirPath, UUID.randomUUID().toString() + ".tmp");
      _localDir = new ProgressableDirectory(FSDirectory.open(_localPath), getProgressable());
      _writer = new IndexWriter(_localDir, _conf.clone());
    } else {
      _localPath = null;
      _localDir = null;
      _writer = new IndexWriter(_finalDir, _conf.clone());
    }
  }

  private Progressable getProgressable() {
    return new Progressable() {
      @Override
      public void progress() {
        Progressable progressable = BlurOutputFormat.getProgressable();
        if (progressable != null) {
          progressable.progress();
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
    if (_countersSetup) {
      _columnCount.increment(record.getColumns().size());
    }
    List<Field> document = RowDocumentUtil.getDoc(_fieldManager, blurRecord.getRowId(), record);
    List<Field> dup = _documents.put(recordId, document);
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
      _localTmpWriter = new IndexWriter(_localTmpDir, _overFlowConf.clone());
      // The local tmp writer has merging disabled so the first document in is
      // going to be doc 0.
      // Therefore the first document added is the prime doc
      List<List<Field>> docs = new ArrayList<List<Field>>(_documents.values());
      docs.get(0).add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
      _localTmpWriter.addDocuments(docs);
    } else {
      _localTmpWriter.addDocuments(_documents.values());
    }
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
      if (_countersSetup) {
        _recordRateCounter.mark(reader.numDocs());
      }
      _writer.addIndexes(reader);
      reader.close();
      resetLocalTmp();
      if (_countersSetup) {
        _rowOverFlowCount.increment(1);
      }
    } else {
      if (_documents.isEmpty()) {
        if (_deletedRowId != null) {
          _writer.addDocument(getDeleteDoc());
          if (_countersSetup) {
            _rowDeleteCount.increment(1);
          }
        }
      } else {
        List<List<Field>> docs = new ArrayList<List<Field>>(_documents.values());
        docs.get(0).add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
        _writer.addDocuments(docs);
        if (_countersSetup) {
          _recordRateCounter.mark(_documents.size());
        }
        _documents.clear();
      }
    }
    _deletedRowId = null;
    if (_countersSetup) {
      _rowRateCounter.mark();
      _rowCount.increment(1);
    }
  }

  private Document getDeleteDoc() {
    Document document = new Document();
    document.add(new StringField(BlurConstants.ROW_ID, _deletedRowId, Store.NO));
    document.add(new StringField(BlurConstants.DELETE_MARKER, BlurConstants.DELETE_MARKER_VALUE, Store.NO));
    return document;
  }

  public void close() throws IOException {
    flush();
    _writer.close();
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
