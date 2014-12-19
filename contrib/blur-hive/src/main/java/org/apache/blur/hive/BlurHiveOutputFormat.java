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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

    TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(jc);
    String conStr = jc.get(BlurSerDe.BLUR_CONTROLLER_CONNECTION_STR);
    final Iface controllerClient = BlurClient.getClient(conStr);
    final String table = tableDescriptor.getName();
    final int numberOfShardsInTable = tableDescriptor.getShardCount();
    final String bulkId = getBulkId(jc);
    return new org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter() {

      private BlurPartitioner _blurPartitioner = new BlurPartitioner();

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
          Iface client = getClient(rowId);
          client.bulkMutateAdd(table, bulkId, rowMutation);
        } catch (BlurException e) {
          throw new IOException(e);
        } catch (TException e) {
          throw new IOException(e);
        }
      }

      private Iface getClient(String rowId) throws BlurException, TException {
        int shard = _blurPartitioner.getShard(rowId, numberOfShardsInTable);
        String shardId = ShardUtil.getShardName(shard);
        return getClientFromShardId(table, shardId);
      }

      private Map<String, String> _shardToServerLayout;
      private Map<String, Iface> _shardClients = new HashMap<String, Iface>();

      private Iface getClientFromShardId(String table, String shardId) throws BlurException, TException {
        if (_shardToServerLayout == null) {
          _shardToServerLayout = controllerClient.shardServerLayout(table);
        }
        return getClientFromConnectionStr(_shardToServerLayout.get(shardId));
      }

      private Iface getClientFromConnectionStr(String connectionStr) {
        Iface iface = _shardClients.get(connectionStr);
        if (iface == null) {
          _shardClients.put(connectionStr, iface = BlurClient.getClient(connectionStr));
        }
        return iface;
      }

      @Override
      public void close(boolean abort) throws IOException {

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
