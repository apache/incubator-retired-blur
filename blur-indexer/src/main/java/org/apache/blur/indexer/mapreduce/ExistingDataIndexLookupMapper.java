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
package org.apache.blur.indexer.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.indexer.BlurIndexCounter;
import org.apache.blur.manager.BlurPartitioner;
import org.apache.blur.manager.writer.SnapshotIndexDeletionPolicy;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.mapreduce.lib.update.IndexKey;
import org.apache.blur.mapreduce.lib.update.IndexValue;
import org.apache.blur.store.BlockCacheDirectoryFactoryV2;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.RowDocumentUtil;
import org.apache.blur.utils.ShardUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;

import com.google.common.io.Closer;

public class ExistingDataIndexLookupMapper extends Mapper<Text, BooleanWritable, IndexKey, IndexValue> {

  private static final Log LOG = LogFactory.getLog(ExistingDataIndexLookupMapper.class);
  private static final String BLUR_SNAPSHOT = "blur.snapshot";
  
  private Counter _existingRecords;
  private Counter _rowLookup;
  private BlurPartitioner _blurPartitioner;
  private Path _tablePath;
  private int _numberOfShardsInTable;
  private Configuration _configuration;
  private String _snapshot;
  private int _indexShard = -1;
  private DirectoryReader _reader;
  private IndexSearcher _indexSearcher;
  private long _totalNumberOfBytes;
  private Closer _closer;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Counter counter = context.getCounter(BlurIndexCounter.LOOKUP_MAPPER);
    counter.increment(1);

    _configuration = context.getConfiguration();
    _existingRecords = context.getCounter(BlurIndexCounter.LOOKUP_MAPPER_EXISTING_RECORDS);
    _rowLookup = context.getCounter(BlurIndexCounter.LOOKUP_MAPPER_ROW_LOOKUP_ATTEMPT);
    _blurPartitioner = new BlurPartitioner();
    TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(_configuration);
    _numberOfShardsInTable = tableDescriptor.getShardCount();
    _tablePath = new Path(tableDescriptor.getTableUri());
    _snapshot = getSnapshot(_configuration);
    _totalNumberOfBytes = _configuration.getLong(LookupBuilderReducer.BLUR_CACHE_DIR_TOTAL_BYTES, 128 * 1024 * 1024);
    _closer = Closer.create();
  }

  @Override
  protected void map(Text key, BooleanWritable value, Context context) throws IOException, InterruptedException {
    if (value.get()) {
      String rowId = key.toString();
      LOG.debug("Looking up rowid [" + rowId + "]");
      _rowLookup.increment(1);
      IndexSearcher indexSearcher = getIndexSearcher(rowId);
      Term term = new Term(BlurConstants.ROW_ID, rowId);
      RowCollector collector = getCollector(context);
      indexSearcher.search(new TermQuery(term), collector);
      LOG.debug("Looking for rowid [" + rowId + "] has [" + collector.records + "] records");
    }
  }

  @Override
  protected void cleanup(Mapper<Text, BooleanWritable, IndexKey, IndexValue>.Context context) throws IOException,
      InterruptedException {
    _closer.close();
  }

  static class RowCollector extends Collector {

    private AtomicReader reader;
    private Mapper<Text, BooleanWritable, IndexKey, IndexValue>.Context _context;
    private Counter _existingRecords;
    int records;

    RowCollector(Mapper<Text, BooleanWritable, IndexKey, IndexValue>.Context context, Counter existingRecords) {
      _context = context;
      _existingRecords = existingRecords;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {

    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      reader = context.reader();
    }

    @Override
    public void collect(int doc) throws IOException {
      Document document = reader.document(doc);
      FetchRecordResult result = RowDocumentUtil.getRecord(document);
      String rowid = result.getRowid();
      Record record = result.getRecord();
      String recordId = record.getRecordId();
      IndexKey oldDataKey = IndexKey.oldData(rowid, recordId);
      try {
        _context.write(oldDataKey, new IndexValue(toBlurRecord(rowid, record)));
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      _existingRecords.increment(1L);
    }

    private BlurRecord toBlurRecord(String rowId, Record record) {
      BlurRecord blurRecord = new BlurRecord();
      blurRecord.setRowId(rowId);
      blurRecord.setRecordId(record.getRecordId());
      blurRecord.setFamily(record.getFamily());
      List<Column> columns = record.getColumns();
      for (Column column : columns) {
        blurRecord.addColumn(column.getName(), column.getValue());
      }
      return blurRecord;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }
  }

  private RowCollector getCollector(Mapper<Text, BooleanWritable, IndexKey, IndexValue>.Context context) {
    return new RowCollector(context, _existingRecords);
  }

  private IndexSearcher getIndexSearcher(String rowId) throws IOException {
    int shard = _blurPartitioner.getShard(rowId, _numberOfShardsInTable);
    if (_indexSearcher != null) {
      if (shard != _indexShard) {
        throw new IOException("Input data is not partitioned correctly.");
      }
      return _indexSearcher;
    } else {
      _indexShard = shard;
      Path shardPath = new Path(_tablePath, ShardUtil.getShardName(_indexShard));
      HdfsDirectory hdfsDirectory = new HdfsDirectory(_configuration, shardPath);
      SnapshotIndexDeletionPolicy policy = new SnapshotIndexDeletionPolicy(_configuration,
          SnapshotIndexDeletionPolicy.getGenerationsPath(shardPath));
      Long generation = policy.getGeneration(_snapshot);
      if (generation == null) {
        hdfsDirectory.close();
        throw new IOException("Snapshot [" + _snapshot + "] not found in shard [" + shardPath + "]");
      }

      BlurConfiguration bc = new BlurConfiguration();
      BlockCacheDirectoryFactoryV2 blockCacheDirectoryFactoryV2 = new BlockCacheDirectoryFactoryV2(bc,
          _totalNumberOfBytes);
      _closer.register(blockCacheDirectoryFactoryV2);
      Directory dir = blockCacheDirectoryFactoryV2.newDirectory("table", "shard", hdfsDirectory, null);

      List<IndexCommit> listCommits = DirectoryReader.listCommits(dir);
      IndexCommit indexCommit = findIndexCommit(listCommits, generation, shardPath);
      _reader = DirectoryReader.open(indexCommit);
      return _indexSearcher = new IndexSearcher(_reader);
    }
  }

  public static IndexCommit findIndexCommit(List<IndexCommit> listCommits, long generation, Path shardDir)
      throws IOException {
    for (IndexCommit commit : listCommits) {
      if (commit.getGeneration() == generation) {
        return commit;
      }
    }
    throw new IOException("Generation [" + generation + "] not found in shard [" + shardDir + "]");
  }

  public static void setSnapshot(Job job, String snapshot) {
    setSnapshot(job.getConfiguration(), snapshot);
  }

  public static void setSnapshot(Configuration configuration, String snapshot) {
    configuration.set(BLUR_SNAPSHOT, snapshot);
  }

  public static String getSnapshot(Configuration configuration) {
    return configuration.get(BLUR_SNAPSHOT);
  }
}
