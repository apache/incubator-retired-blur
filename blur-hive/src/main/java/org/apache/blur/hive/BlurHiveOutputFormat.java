/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.blur.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.manager.BlurPartitioner;
import org.apache.blur.mapreduce.lib.BlurColumn;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.ShardUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class BlurHiveOutputFormat implements HiveOutputFormat<Text, BlurRecord> {

  private static final String BLUR_BULK_MUTATE_ID = "blur.bulk.mutate.id";

  public static String getBulkId(Configuration conf) {
    return conf.get(BLUR_BULK_MUTATE_ID);
  }

  public static void setBulkId(Configuration conf, String bulkId) {
    conf.set(BLUR_BULK_MUTATE_ID, bulkId);
  }

  @Override
  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

  }

  @Override
  public RecordWriter<Text, BlurRecord> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String name,
      Progressable progressable) throws IOException {
    throw new RuntimeException("Should never be called.");
  }

  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
      Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
      Progressable progress) throws IOException {
    if (BlurSerDe.shouldUseMRWorkingPath(jc)) {
      return getMrWorkingPathWriter(jc);
    }
    return getBulkRecordWriter(jc);
  }

  private org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getMrWorkingPathWriter(
      Configuration configuration) throws IOException {
    String workingPathStr = configuration.get(BlurSerDe.BLUR_MR_UPDATE_WORKING_PATH);
    Path workingPath = new Path(workingPathStr);
    Path tmpDir = new Path(workingPath, "tmp");
    FileSystem fileSystem = tmpDir.getFileSystem(configuration);
    String loadId = configuration.get(BlurSerDe.BLUR_MR_LOAD_ID);
    Path loadPath = new Path(tmpDir, loadId);

    final Writer writer = new SequenceFile.Writer(fileSystem, configuration, new Path(loadPath, UUID.randomUUID()
        .toString()), Text.class, BlurRecord.class);

    return new org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter() {

      @Override
      public void write(Writable w) throws IOException {
        BlurRecord blurRecord = (BlurRecord) w;
        String rowId = blurRecord.getRowId();
        writer.append(new Text(rowId), blurRecord);
      }

      @Override
      public void close(boolean abort) throws IOException {
        writer.close();
      }
    };
  }

  private org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getBulkRecordWriter(Configuration configuration)
      throws IOException {
    TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(configuration);
    String conStr = configuration.get(BlurSerDe.BLUR_CONTROLLER_CONNECTION_STR);
    final Iface controllerClient = BlurClient.getClient(conStr);
    final String table = tableDescriptor.getName();
    final int numberOfShardsInTable = tableDescriptor.getShardCount();
    final String bulkId = getBulkId(configuration);
    return new org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter() {

      private BlurPartitioner _blurPartitioner = new BlurPartitioner();
      private Map<String, List<RowMutation>> _serverBatches = new ConcurrentHashMap<String, List<RowMutation>>();
      private int _capacity = 100;
      private Map<String, String> _shardToServerLayout;

      @Override
      public void write(Writable w) throws IOException {
        BlurRecord blurRecord = (BlurRecord) w;
        String rowId = blurRecord.getRowId();
        RowMutation rowMutation = new RowMutation();
        rowMutation.setTable(table);
        rowMutation.setRowId(rowId);
        rowMutation.setRowMutationType(RowMutationType.UPDATE_ROW);
        rowMutation.addToRecordMutations(new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD,
            toRecord(blurRecord)));

        try {
          String server = getServer(rowId);
          List<RowMutation> batch = _serverBatches.get(server);
          if (batch == null) {
            _serverBatches.put(server, batch = new ArrayList<RowMutation>(_capacity));
          }
          batch.add(rowMutation);
          checkForFlush(_capacity);
        } catch (BlurException e) {
          throw new IOException(e);
        } catch (TException e) {
          throw new IOException(e);
        }
      }

      @Override
      public void close(boolean abort) throws IOException {
        try {
          checkForFlush(1);
        } catch (BlurException e) {
          throw new IOException(e);
        } catch (TException e) {
          throw new IOException(e);
        }
      }

      private void checkForFlush(int max) throws BlurException, TException {
        for (Entry<String, List<RowMutation>> e : _serverBatches.entrySet()) {
          String server = e.getKey();
          List<RowMutation> batch = e.getValue();
          if (batch.size() >= max) {
            Iface client = BlurClient.getClient(server);
            client.bulkMutateAddMultiple(bulkId, batch);
            batch.clear();
          }
        }
      }

      private String getServer(String rowId) throws BlurException, TException {
        int shard = _blurPartitioner.getShard(rowId, numberOfShardsInTable);
        String shardId = ShardUtil.getShardName(shard);
        return getServerFromShardId(table, shardId);
      }

      private String getServerFromShardId(String table, String shardId) throws BlurException, TException {
        if (_shardToServerLayout == null) {
          _shardToServerLayout = controllerClient.shardServerLayout(table);
        }
        return _shardToServerLayout.get(shardId);
      }

    };
  }

  protected Record toRecord(BlurRecord blurRecord) {
    return new Record(blurRecord.getRecordId(), blurRecord.getFamily(), toColumns(blurRecord.getColumns()));
  }

  private List<Column> toColumns(List<BlurColumn> columns) {
    List<Column> result = new ArrayList<Column>();
    for (BlurColumn blurColumn : columns) {
      result.add(new Column(blurColumn.getName(), blurColumn.getValue()));
    }
    return result;
  }

}
