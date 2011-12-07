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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurUtil;

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
  private String _spinLockPath;
  private String _zookeeperConnectionStr;
  private int _maxNumberOfConcurrentCopies;
  private int _maxNumSegments = -1;

  public String getShardName(TaskAttemptContext context) {
    TaskAttemptID taskAttemptID = context.getTaskAttemptID();
    int id = taskAttemptID.getTaskID().getId();
    return BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, id);
  }

  public Path getDirectoryPath(TaskAttemptContext context) {
    String shardName = getShardName(context);
    return new Path(new Path(_tableDescriptor.tableUri), shardName);
  }

  public int getNumReducers(Configuration configuration) {
    Path tablePath = new Path(_tableDescriptor.tableUri);
    try {
      int num = _tableDescriptor.shardCount;
      FileSystem fileSystem = FileSystem.get(tablePath.toUri(), configuration);
      if (!fileSystem.exists(tablePath)) {
        return num;
      }
      FileStatus[] files = fileSystem.listStatus(tablePath);
      int shardCount = 0;
      for (FileStatus fileStatus : files) {
        if (fileStatus.isDir()) {
          String name = fileStatus.getPath().getName();
          if (name.startsWith(BlurConstants.SHARD_PREFIX)) {
            shardCount++;
          }
        }
      }
      
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
    setupZookeeper();
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

  private void setupZookeeper() throws IOException {
    if (_zookeeperConnectionStr == null || _spinLockPath == null) {
      LOG.warn("Cannot setup spin locks, please set zookeeper connection str and spin lock path.");
      return;
    }
    ZooKeeper zooKeeper = new ZooKeeper(_zookeeperConnectionStr,60000,new Watcher(){
      @Override
      public void process(WatchedEvent event) {
        
      }
    });
    try {
      Stat stat = zooKeeper.exists(_spinLockPath, false);
      if (stat == null) {
        zooKeeper.create(_spinLockPath, Integer.toString(_maxNumberOfConcurrentCopies).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else {
        zooKeeper.setData(_spinLockPath, Integer.toString(_maxNumberOfConcurrentCopies).getBytes(), -1);
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static BlurTask read(Configuration configuration) throws IOException {  
    byte[] blurTaskBs = Base64.decodeBase64(configuration.get(BLUR_BLURTASK));
    BlurTask blurTask = new BlurTask();
    blurTask.readFields(new DataInputStream(new ByteArrayInputStream(blurTaskBs)));
    return blurTask;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    _zookeeperConnectionStr = readString(input);
    _spinLockPath = readString(input);
    _maxNumberOfConcurrentCopies = input.readInt();
    _maxRecordCount = input.readLong();
    _ramBufferSizeMB = input.readInt();
    _maxNumSegments = input.readInt();
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

  private String readString(DataInput input) throws IOException {
    int length = input.readInt();
    byte[] buf = new byte[length];
    input.readFully(buf);
    return new String(buf);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    writeString(output,_zookeeperConnectionStr);
    writeString(output,_spinLockPath);
    output.writeInt(_maxNumberOfConcurrentCopies);
    output.writeLong(_maxRecordCount);
    output.writeInt(_ramBufferSizeMB);
    output.writeInt(_maxNumSegments);
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

  private void writeString(DataOutput output, String s) throws IOException {
    byte[] bs = s.getBytes();
    output.writeInt(bs.length);
    output.write(bs);
  }

  public int getMaxRecordsPerRow() {
    return _maxRecordsPerRow;
  }

  public void setMaxRecordsPerRow(int maxRecordsPerRow) {
    _maxRecordsPerRow = maxRecordsPerRow;
  }

  public String getZookeeperConnectionStr() {
    return _zookeeperConnectionStr;
  }

  public String getSpinLockPath() {
    return _spinLockPath;
  }

  public void setSpinLockPath(String spinLockPath) {
    _spinLockPath = spinLockPath;
  }

  public void setZookeeperConnectionStr(String zookeeperConnectionStr) {
    _zookeeperConnectionStr = zookeeperConnectionStr;
  }

  public void setMaxNumberOfConcurrentCopies(int maxNumberOfConcurrentCopies) {
    _maxNumberOfConcurrentCopies = maxNumberOfConcurrentCopies;
  }
  
  public int getMaxNumberOfConcurrentCopies() {
    return _maxNumberOfConcurrentCopies;
  }

  public int getMaxNumSegments() {
    return _maxNumSegments;    
  }

  public void setMaxNumSegments(int maxNumSegments) {
    _maxNumSegments = maxNumSegments;
  }
}
