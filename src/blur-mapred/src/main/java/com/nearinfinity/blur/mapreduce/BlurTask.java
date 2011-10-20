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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import com.nearinfinity.blur.BlurShardName;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurTask implements Writable {

  private static final String BLUR_BLURTASK = "blur.blurtask";
  private static final Log LOG = LogFactory.getLog(BlurTask.class);
  
  public static String getCounterGroupName() {
    return "Blur";
  }

  public static String getRowCounterName() {
    return "Rows";
  }

  public static String getFieldCounterName() {
    return "Fields";
  }

  public static String getRecordCounterName() {
    return "Records";
  }

  public static String getRowBreakCounterName() {
    return "Row Retries";
  }

  public static String getRowFailureCounterName() {
    return "Row Failures";
  }
  
  private int _ramBufferSizeMB = 256;
  private long _maxRecordCount = Long.MAX_VALUE;
  private TableDescriptor _tableDescriptor;
  private int _maxRecordsPerRow = 16384;

  public String getShardName(TaskAttemptContext context) {
    TaskAttemptID taskAttemptID = context.getTaskAttemptID();
    int id = taskAttemptID.getTaskID().getId();
    return BlurShardName.getShardName(BlurConstants.SHARD_PREFIX, id);
  }

  public Path getDirectoryPath(TaskAttemptContext context) {
    String shardName = getShardName(context);
    return new Path(new Path(new Path(_tableDescriptor.tableUri), _tableDescriptor.name), shardName);
  }

  public int getNumReducers(Configuration configuration) {
    Path shardPath = new Path(new Path(_tableDescriptor.tableUri), _tableDescriptor.name);
    try {
      FileSystem fileSystem = FileSystem.get(configuration);
      FileStatus[] files = fileSystem.listStatus(shardPath);
      int shardCount = 0;
      for (FileStatus fileStatus : files) {
        if (fileStatus.isDir()) {
          shardCount++;
        }
      }
      int num = _tableDescriptor.shardCount;
      if (shardCount == 0) {
        return num;
      }
      if (shardCount != num) {
        LOG.warn("Asked for " + num + " reducers, but existing table " + _tableDescriptor.name + " has " + shardCount + " shards. Using " + shardCount + " reducers");
      }
      return shardCount;
    } catch (IOException e) {
      throw new RuntimeException("Unable to connect to filesystem", e);
    }
  }
  
  public int getRamBufferSizeMB() {
    return _ramBufferSizeMB;
  }

  public void setRamBufferSizeMB(int ramBufferSizeMB) {
    _ramBufferSizeMB = ramBufferSizeMB;
  }

  public long getMaxRecordCount() {
    return _maxRecordCount;
  }

  public void setMaxRecordCount(long maxRecordCount) {
    _maxRecordCount = maxRecordCount;
  }

  public void setTableDescriptor(TableDescriptor tableDescriptor) {
    _tableDescriptor = tableDescriptor;
  }

  public TableDescriptor getTableDescriptor() {
    return _tableDescriptor;
  }

  public Job configureJob(Configuration configuration) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(os);
    write(output);
    output.close();
    String blurTask = new String(Base64.encodeBase64(os.toByteArray()));
    configuration.set(BLUR_BLURTASK, blurTask);
    
    Job job = new Job(configuration, "Blur Indexer");
    job.setReducerClass(BlurReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BlurRecord.class);
    job.setNumReduceTasks(getNumReducers(configuration));
    return job;
  }

  public static BlurTask read(Configuration configuration) throws IOException {  
    byte[] blurTaskBs = Base64.decodeBase64(configuration.get(BLUR_BLURTASK));
    BlurTask blurTask = new BlurTask();
    blurTask.readFields(new DataInputStream(new ByteArrayInputStream(blurTaskBs)));
    return blurTask;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    _maxRecordCount = input.readLong();
    _ramBufferSizeMB = input.readInt();
    byte[] data = new byte[input.readInt()];
    input.readFully(data);
    ByteArrayInputStream is = new ByteArrayInputStream(data);
    TIOStreamTransport trans = new TIOStreamTransport(is);
    TBinaryProtocol protocol = new TBinaryProtocol(trans);
    _tableDescriptor = new TableDescriptor();
    try {
      _tableDescriptor.read(protocol);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(_maxRecordCount);
    output.writeInt(_ramBufferSizeMB);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    TIOStreamTransport trans = new TIOStreamTransport(os);
    TBinaryProtocol protocol = new TBinaryProtocol(trans);
    try {
      _tableDescriptor.write(protocol);
    } catch (TException e) {
      throw new IOException(e);
    }
    os.close();
    byte[] bs = os.toByteArray();
    output.writeInt(bs.length);
    output.write(bs);
  }

  public int getMaxRecordsPerRow() {
    return _maxRecordsPerRow;
  }

  public void setMaxRecordsPerRow(int maxRecordsPerRow) {
    _maxRecordsPerRow = maxRecordsPerRow;
  }
}
