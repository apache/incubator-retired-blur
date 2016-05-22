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
package org.apache.blur.mapreduce.lib.update;

import java.io.IOException;
import java.util.List;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.manager.BlurPartitioner;
import org.apache.blur.manager.writer.SnapshotIndexDeletionPolicy;
import org.apache.blur.mapreduce.lib.BlurInputFormat;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.mapreduce.lib.update.MergeSortRowIdMatcher.Action;
import org.apache.blur.store.BlockCacheDirectoryFactoryV2;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.ShardUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;

import com.google.common.io.Closer;

public class LookupBuilderReducer extends Reducer<Text, NullWritable, Text, BooleanWritable> {

  public static final String BLUR_CACHE_DIR_TOTAL_BYTES = "blur.cache.dir.total.bytes";
  private Counter _rowIds;
  private Counter _rowIdsToUpdate;

  private MergeSortRowIdMatcher _matcher;
  private int _numberOfShardsInTable;
  private Configuration _configuration;
  private String _snapshot;
  private Path _tablePath;
  private Counter _rowIdsFromIndex;
  private long _totalNumberOfBytes;
  private Action _action;
  private Closer _closer;
  private Path _cachePath;
  private String _table;
  private Writer _writer;

  @Override
  protected void setup(Reducer<Text, NullWritable, Text, BooleanWritable>.Context context) throws IOException,
      InterruptedException {
    _configuration = context.getConfiguration();
    _rowIds = context.getCounter(BlurIndexCounter.ROW_IDS_FROM_NEW_DATA);
    _rowIdsToUpdate = context.getCounter(BlurIndexCounter.ROW_IDS_TO_UPDATE_FROM_NEW_DATA);
    _rowIdsFromIndex = context.getCounter(BlurIndexCounter.ROW_IDS_FROM_INDEX);
    TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(_configuration);
    _numberOfShardsInTable = tableDescriptor.getShardCount();
    _tablePath = new Path(tableDescriptor.getTableUri());
    _snapshot = MapperForExistingDataWithIndexLookup.getSnapshot(_configuration);
    _totalNumberOfBytes = _configuration.getLong(BLUR_CACHE_DIR_TOTAL_BYTES, 128 * 1024 * 1024);
    _cachePath = BlurInputFormat.getLocalCachePath(_configuration);
    _table = tableDescriptor.getName();
    _closer = Closer.create();
  }

  @Override
  protected void reduce(Text rowId, Iterable<NullWritable> nothing,
      Reducer<Text, NullWritable, Text, BooleanWritable>.Context context) throws IOException, InterruptedException {
    if (_matcher == null) {
      _matcher = getMergeSortRowIdMatcher(rowId, context);
    }
    if (_writer == null) {
      _writer = getRowIdWriter(rowId, context);
    }
    _writer.append(rowId, NullWritable.get());
    _rowIds.increment(1);
    if (_action == null) {
      _action = new Action() {
        @Override
        public void found(Text rowId) throws IOException {
          _rowIdsToUpdate.increment(1);
          try {
            context.write(rowId, new BooleanWritable(true));
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
      };
    }
    _matcher.lookup(rowId, _action);
  }

  private Writer getRowIdWriter(Text rowId, Reducer<Text, NullWritable, Text, BooleanWritable>.Context context)
      throws IOException {
    BlurPartitioner blurPartitioner = new BlurPartitioner();
    int shard = blurPartitioner.getShard(rowId, _numberOfShardsInTable);
    String shardName = ShardUtil.getShardName(shard);
    Path cachePath = MergeSortRowIdMatcher.getCachePath(_cachePath, _table, shardName);
    Configuration configuration = context.getConfiguration();
    String uuid = configuration.get(FasterDriver.BLUR_UPDATE_ID);
    Path tmpPath = new Path(cachePath, uuid + "_" + getAttemptString(context));
    return _closer.register(MergeSortRowIdMatcher.createWriter(_configuration, tmpPath));
  }

  private String getAttemptString(Reducer<Text, NullWritable, Text, BooleanWritable>.Context context) {
    TaskAttemptID taskAttemptID = context.getTaskAttemptID();
    return taskAttemptID.toString();
  }

  @Override
  protected void cleanup(Reducer<Text, NullWritable, Text, BooleanWritable>.Context context) throws IOException,
      InterruptedException {
    _closer.close();
  }

  private MergeSortRowIdMatcher getMergeSortRowIdMatcher(Text rowId,
      Reducer<Text, NullWritable, Text, BooleanWritable>.Context context) throws IOException {
    BlurPartitioner blurPartitioner = new BlurPartitioner();
    int shard = blurPartitioner.getShard(rowId, _numberOfShardsInTable);
    String shardName = ShardUtil.getShardName(shard);

    Path shardPath = new Path(_tablePath, shardName);
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
    IndexCommit indexCommit = MapperForExistingDataWithIndexLookup.findIndexCommit(listCommits, generation, shardPath);
    DirectoryReader reader = DirectoryReader.open(indexCommit);
    _rowIdsFromIndex.setValue(getTotalNumberOfRowIds(reader));

    Path cachePath = MergeSortRowIdMatcher.getCachePath(_cachePath, _table, shardName);
    return new MergeSortRowIdMatcher(dir, generation, _configuration, cachePath, context);
  }

  private long getTotalNumberOfRowIds(DirectoryReader reader) throws IOException {
    long total = 0;
    List<AtomicReaderContext> leaves = reader.leaves();
    for (AtomicReaderContext context : leaves) {
      AtomicReader atomicReader = context.reader();
      Terms terms = atomicReader.terms(BlurConstants.ROW_ID);
      long expectedInsertions = terms.size();
      if (expectedInsertions < 0) {
        return -1;
      }
      total += expectedInsertions;
    }
    return total;
  }
}
