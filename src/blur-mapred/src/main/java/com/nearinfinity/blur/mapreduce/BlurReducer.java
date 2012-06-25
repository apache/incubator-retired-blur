/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.mapreduce;

import static com.nearinfinity.blur.lucene.LuceneConstant.LUCENE_VERSION;
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC;
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC_VALUE;
import static com.nearinfinity.blur.utils.BlurConstants.RECORD_ID;
import static com.nearinfinity.blur.utils.BlurConstants.ROW_ID;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.IOUtils;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.index.DirectIODirectory;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.mapreduce.BlurMutate.MUTATE_TYPE;
import com.nearinfinity.blur.mapreduce.BlurTask.INDEXING_TYPE;
import com.nearinfinity.blur.store.compressed.CompressedFieldDataDirectory;
import com.nearinfinity.blur.store.hdfs.HdfsDirectory;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurUtil;
import com.nearinfinity.blur.utils.Converter;
import com.nearinfinity.blur.utils.IterableConverter;
import com.nearinfinity.blur.utils.RowIndexWriter;

public class BlurReducer extends Reducer<BytesWritable, BlurMutate, BytesWritable, BlurMutate> {

  static class LuceneFileComparator implements Comparator<String> {

    private Directory _directory;

    public LuceneFileComparator(Directory directory) {
      _directory = directory;
    }

    @Override
    public int compare(String o1, String o2) {
      try {
        long fileLength1 = _directory.fileLength(o1);
        long fileLength2 = _directory.fileLength(o2);
        if (fileLength1 == fileLength2) {
          return o1.compareTo(o2);
        }
        return (int) (fileLength2 - fileLength1);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  };

  protected static final Log LOG = LogFactory.getLog(BlurReducer.class);
  protected static final Field PRIME_FIELD = new Field(PRIME_DOC, PRIME_DOC_VALUE, Store.NO, Index.ANALYZED_NO_NORMS);
  protected static final long REPORT_PERIOD = TimeUnit.SECONDS.toMillis(10);
  protected static final double MB = 1024 * 1024;
  protected IndexWriter _writer;
  protected Directory _directory;
  protected BlurAnalyzer _analyzer;
  protected BlurTask _blurTask;

  protected Counter _recordCounter;
  protected Counter _rowCounter;
  protected Counter _fieldCounter;
  protected Counter _rowBreak;
  protected Counter _rowFailures;
  protected StringBuilder _builder = new StringBuilder();
  protected byte[] _copyBuf;
  protected Configuration _configuration;
  protected long _start;
  protected long _previousRow;
  protected long _previousRecord;
  protected long _prev;
  protected IndexReader _reader;
  protected Map<String, Document> _newDocs = new HashMap<String, Document>();
  protected Set<String> _recordIdsToDelete = new HashSet<String>();
  protected Term _rowIdTerm = new Term(BlurConstants.ROW_ID);
  protected ZooKeeper _zookeeper;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    _blurTask = BlurTask.read(context.getConfiguration());
    _configuration = context.getConfiguration();
    setupCounters(context);
    setupAnalyzer(context);
    setupDirectory(context);
    setupWriter(context);
    if (_blurTask.getIndexingType() == INDEXING_TYPE.UPDATE) {
      _reader = IndexReader.open(_directory, true);
    }
  }

  protected void setupCounters(Context context) {
    _rowCounter = context.getCounter(BlurTask.getCounterGroupName(), BlurTask.getRowCounterName());
    _recordCounter = context.getCounter(BlurTask.getCounterGroupName(), BlurTask.getRecordCounterName());
    _fieldCounter = context.getCounter(BlurTask.getCounterGroupName(), BlurTask.getFieldCounterName());
    _rowBreak = context.getCounter(BlurTask.getCounterGroupName(), BlurTask.getRowBreakCounterName());
    _rowFailures = context.getCounter(BlurTask.getCounterGroupName(), BlurTask.getRowFailureCounterName());
    _start = System.currentTimeMillis();
    _prev = System.currentTimeMillis();
  }

  @Override
  protected void reduce(BytesWritable key, Iterable<BlurMutate> values, Context context) throws IOException, InterruptedException {
    if (!index(key, values, context)) {
      _rowFailures.increment(1);
    }
  }

  protected boolean index(BytesWritable key, Iterable<BlurMutate> values, Context context) throws IOException {
    int recordCount = 0;
    _newDocs.clear();
    _recordIdsToDelete.clear();
    boolean rowIdSet = false;

    for (BlurMutate mutate : values) {
      BlurRecord record = mutate.getRecord();
      if (!rowIdSet) {
        String rowId = record.getRowId();
        _rowIdTerm = _rowIdTerm.createTerm(rowId);
        rowIdSet = true;
      }
      if (mutate.getMutateType() == MUTATE_TYPE.DELETE) {
        _recordIdsToDelete.add(record.getRecordId());
        continue;
      }
      Document document = toDocument(record, _builder);
      _newDocs.put(record.getRecordId(), document);

      context.progress();
      recordCount++;
      if (recordCount >= _blurTask.getMaxRecordsPerRow()) {
        return false;
      }
      if (_blurTask.getIndexingType() == INDEXING_TYPE.UPDATE) {
        fetchOldRecords();
      }
    }

    Collection<Document> docs = documentsToIndex(new ArrayList<Document>(_newDocs.values()));
    for (Document document : docs) {
      document.add(PRIME_FIELD);
      break;
    }

    switch (_blurTask.getIndexingType()) {
    case REBUILD:
      _writer.addDocuments(docs);
      break;
    case UPDATE:
      _writer.updateDocuments(_rowIdTerm, docs);
    default:
      break;
    }

    _recordCounter.increment(recordCount);
    _rowCounter.increment(1);
    if (_prev + REPORT_PERIOD < System.currentTimeMillis()) {
      long records = _recordCounter.getValue();
      long rows = _rowCounter.getValue();

      long now = System.currentTimeMillis();

      double overAllSeconds = (now - _start) / 1000.0;
      double overAllRecordRate = records / overAllSeconds;
      double overAllRowsRate = rows / overAllSeconds;

      double seconds = (now - _prev) / 1000.0;
      double recordRate = (records - _previousRecord) / seconds;
      double rowsRate = (rows - _previousRow) / seconds;

      String status = String.format("Totals [%d Row, %d Records], Avg Rates [%.1f Row/s, %.1f Records/s] Rates [%.1f Row/s, %.1f Records/s]", rows, records, overAllRowsRate,
          overAllRecordRate, rowsRate, recordRate);

      LOG.info(status);
      context.setStatus(status);

      _previousRecord = records;
      _previousRow = rows;
      _prev = now;
    }
    return true;
  }

  protected Collection<Document> documentsToIndex(Collection<Document> list) {
    return list;
  }

  protected void fetchOldRecords() throws IOException {
    TermDocs termDocs = _reader.termDocs(_rowIdTerm);
    // find all records for row that are not deleted.
    while (termDocs.next()) {
      int doc = termDocs.doc();
      if (!_reader.isDeleted(doc)) {
        Document document = _reader.document(doc);
        String recordId = document.get(RECORD_ID);
        // add them to the new records if the new records do not contain them.
        if (!_newDocs.containsKey(recordId)) {
          _newDocs.put(recordId, document);
        }
      }
    }

    // delete all records that should be removed.
    for (String recordId : _recordIdsToDelete) {
      _newDocs.remove(recordId);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    switch (_blurTask.getIndexingType()) {
    case UPDATE:
      cleanupFromUpdate(context);
      return;
    case REBUILD:
      cleanupFromRebuild(context);
      return;
    default:
      break;
    }
  }

  protected void cleanupFromUpdate(Context context) throws IOException {
    _writer.commit();
    _writer.close();
  }

  protected void cleanupFromRebuild(Context context) throws IOException, InterruptedException {
    _writer.commit();
    _writer.close();

    IndexReader reader = IndexReader.open(_directory);

    TableDescriptor descriptor = _blurTask.getTableDescriptor();

    Path directoryPath = _blurTask.getDirectoryPath(context);
    remove(directoryPath);

    NoLockFactory lockFactory = NoLockFactory.getNoLockFactory();

    Directory destDirectory = getDestDirectory(descriptor, directoryPath);
    destDirectory.setLockFactory(lockFactory);

    boolean optimize = _blurTask.getOptimize();

    if (optimize) {
      context.setStatus("Starting Copy-Optimize Phase");
      IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, _analyzer);
      TieredMergePolicy policy = (TieredMergePolicy) conf.getMergePolicy();
      policy.setUseCompoundFile(false);
      long s = System.currentTimeMillis();
      IndexWriter writer = new IndexWriter(getBiggerBuffers(destDirectory), conf);
      writer.addIndexes(reader);
      writer.close();
      long e = System.currentTimeMillis();
      context.setStatus("Copying phase took [" + (e - s) + " ms]");
      LOG.info("Copying phase took [" + (e - s) + " ms]");
    } else {
      context.setStatus("Starting Copy-Optimize Phase");
      long s = System.currentTimeMillis();
      List<String> files = getFilesOrderedBySize(_directory);
      long totalBytesToCopy = getTotalBytes(_directory);
      long totalBytesCopied = 0;
      long startTime = System.currentTimeMillis();
      for (String file : files) {
        totalBytesCopied += copy(_directory, destDirectory, file, file, context, totalBytesCopied, totalBytesToCopy, startTime);
      }
      long e = System.currentTimeMillis();
      context.setStatus("Copying phase took [" + (e - s) + " ms]");
      LOG.info("Copying phase took [" + (e - s) + " ms]");
    }
  }

  protected Directory getBiggerBuffers(Directory destDirectory) {
    return new BufferedDirectory(destDirectory, 32768);
  }

  protected Directory getDestDirectory(TableDescriptor descriptor, Path directoryPath) throws IOException {
    String compressionClass = descriptor.compressionClass;
    int compressionBlockSize = descriptor.getCompressionBlockSize();
    if (compressionClass == null) {
      compressionClass = "org.apache.hadoop.io.compress.DefaultCodec";
    }
    // if (compressionBlockSize == 0) {
    compressionBlockSize = 32768;
    // }
    HdfsDirectory dir = new HdfsDirectory(directoryPath);
    return new CompressedFieldDataDirectory(dir, getInstance(compressionClass), compressionBlockSize);
  }

  protected CompressionCodec getInstance(String compressionClass) throws IOException {
    try {
      CompressionCodec codec = (CompressionCodec) Class.forName(compressionClass).newInstance();
      if (codec instanceof Configurable) {
        Configurable configurable = (Configurable) codec;
        configurable.setConf(_configuration);
      }
      return codec;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  protected void remove(Path directoryPath) throws IOException {
    FileSystem fileSystem = FileSystem.get(directoryPath.toUri(), _configuration);
    fileSystem.delete(directoryPath, true);
  }

  protected long getTotalBytes(Directory directory) throws IOException {
    long total = 0;
    for (String file : directory.listAll()) {
      total += directory.fileLength(file);
    }
    return total;
  }

  protected long copy(Directory from, Directory to, String src, String dest, Context context, long totalBytesCopied, long totalBytesToCopy, long startTime) throws IOException {
    IndexOutput os = to.createOutput(dest);
    IndexInput is = from.openInput(src);
    IOException priorException = null;
    try {
      return copyBytes(is, os, is.length(), context, totalBytesCopied, totalBytesToCopy, startTime, src);
    } catch (IOException ioe) {
      priorException = ioe;
    } finally {
      IOUtils.closeWhileHandlingException(priorException, os, is);
    }
    return 0;// this should never be called
  }

  protected long copyBytes(IndexInput in, IndexOutput out, long numBytes, Context context, long totalBytesCopied, long totalBytesToCopy, long startTime, String src)
      throws IOException {
    if (_copyBuf == null) {
      _copyBuf = new byte[BufferedIndexInput.BUFFER_SIZE];
    }
    long start = System.currentTimeMillis();
    long copied = 0;
    while (numBytes > 0) {
      if (start + REPORT_PERIOD < System.currentTimeMillis()) {
        report(context, totalBytesCopied + copied, totalBytesToCopy, startTime, src);
        start = System.currentTimeMillis();
      }
      final int toCopy = (int) (numBytes > _copyBuf.length ? _copyBuf.length : numBytes);
      in.readBytes(_copyBuf, 0, toCopy);
      out.writeBytes(_copyBuf, 0, toCopy);
      numBytes -= toCopy;
      copied += toCopy;
      context.progress();
    }
    return copied;
  }

  protected List<String> getFilesOrderedBySize(final Directory directory) throws IOException {
    List<String> files = new ArrayList<String>(Arrays.asList(directory.listAll()));
    Collections.sort(files, new LuceneFileComparator(_directory));
    return files;
  }

  protected void setupDirectory(Context context) throws IOException {
    TableDescriptor descriptor = _blurTask.getTableDescriptor();
    switch (_blurTask.getIndexingType()) {
    case UPDATE:
      Path directoryPath = _blurTask.getDirectoryPath(context);
      _directory = getDestDirectory(descriptor, directoryPath);

      NoLockFactory lockFactory = NoLockFactory.getNoLockFactory();
      _directory.setLockFactory(lockFactory);
      return;
    case REBUILD:
      File dir = new File(System.getProperty("java.io.tmpdir"));
      File path = new File(dir, "index");
      rm(path);
      LOG.info("Using local path [" + path + "] for indexing.");
      String compressionClass = descriptor.compressionClass;
      int compressionBlockSize = descriptor.getCompressionBlockSize();
      if (compressionClass == null) {
        compressionClass = "org.apache.hadoop.io.compress.DefaultCodec";
      }

      Directory localDirectory = FSDirectory.open(path);
      // if (compressionBlockSize == 0) {
      compressionBlockSize = 32768;
      // }
      CompressedFieldDataDirectory compressedFieldDataDirectory = new CompressedFieldDataDirectory(DirectIODirectory.wrap(localDirectory), getInstance(compressionClass),
          compressionBlockSize);
      _directory = new ProgressableDirectory(compressedFieldDataDirectory, context);
      return;
    default:
      break;
    }
  }

  protected String getNodeName(Context context) {
    return context.getTaskAttemptID().toString();
  }

  protected void rm(File path) {
    if (!path.exists()) {
      return;
    }
    if (path.isDirectory()) {
      for (File f : path.listFiles()) {
        rm(f);
      }
    }
    path.delete();
  }

  protected <T> T nullCheck(T o) {
    if (o == null) {
      throw new NullPointerException();
    }
    return o;
  }

  protected void setupWriter(Context context) throws IOException {
    nullCheck(_directory);
    nullCheck(_analyzer);
    IndexWriterConfig config = new IndexWriterConfig(LUCENE_VERSION, _analyzer);
    config.setSimilarity(new FairSimilarity());
    config.setRAMBufferSizeMB(_blurTask.getRamBufferSizeMB());
    TieredMergePolicy mergePolicy = (TieredMergePolicy) config.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    _writer = new IndexWriter(_directory, config);
  }

  protected void setupAnalyzer(Context context) {
    _analyzer = new BlurAnalyzer(_blurTask.getTableDescriptor().getAnalyzerDefinition());
  }

  protected Document toDocument(BlurRecord record, StringBuilder builder) {
    Document document = new Document();
    document.add(new Field(ROW_ID, record.getRowId(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    document.add(new Field(RECORD_ID, record.getRecordId(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    String columnFamily = record.getFamily();
    RowIndexWriter.addColumns(document, _analyzer, builder, columnFamily, new IterableConverter<BlurColumn, Column>(record.getColumns(), new Converter<BlurColumn, Column>() {
      @Override
      public Column convert(BlurColumn from) throws Exception {
        _fieldCounter.increment(1);
        return new Column(from.getName(), from.getValue());
      }
    }));
    return document;
  }

  protected static void report(Context context, long totalBytesCopied, long totalBytesToCopy, long startTime, String src) {
    long now = System.currentTimeMillis();
    double seconds = (now - startTime) / 1000.0;
    double rate = totalBytesCopied / seconds;
    String time = estimateTimeToComplete(rate, totalBytesCopied, totalBytesToCopy);

    String status = String.format("%.1f Complete - Time Remaining [%s s], Copy rate [%.1f MB/s], Total Copied [%.1f MB], Total To Copy [%.1f MB]", getPerComplete(totalBytesCopied,
        totalBytesToCopy), time, getMb(rate), getMb(totalBytesCopied), getMb(totalBytesToCopy));
    LOG.info(status);
    context.setStatus(status);
  }

  protected static double getPerComplete(long totalBytesCopied, long totalBytesToCopy) {
    return ((double) totalBytesCopied / (double) totalBytesToCopy) * 100.0;
  }

  protected static double getMb(double b) {
    return b / MB;
  }

  protected static String estimateTimeToComplete(double rate, long totalBytesCopied, long totalBytesToCopy) {
    long whatsLeft = totalBytesToCopy - totalBytesCopied;
    long secondsLeft = (long) (whatsLeft / rate);
    return BlurUtil.humanizeTime(secondsLeft, TimeUnit.SECONDS);
  }
}
