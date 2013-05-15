package org.apache.blur.mapreduce;

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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.clusterstatus.ZookeeperClusterStatus;
import org.apache.blur.manager.clusterstatus.ZookeeperPathConstants;
import org.apache.blur.mapreduce.lib.BlurMutate;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

@Deprecated
public class BlurTask implements Writable {

  public enum INDEXING_TYPE {
    REBUILD, UPDATE
  }

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
  private boolean _optimize = true;
  private INDEXING_TYPE _indexingType = INDEXING_TYPE.REBUILD;
  private transient ZooKeeper _zooKeeper;

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
        LOG.warn("Asked for " + num + " reducers, but existing table " + _tableDescriptor.name + " has " + shardCount
            + " shards. Using " + shardCount + " reducers");
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
    if (getIndexingType() == INDEXING_TYPE.UPDATE) {
      checkTable();
    }
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(os);
    write(output);
    output.close();
    String blurTask = new String(Base64.encodeBase64(os.toByteArray()));
    configuration.set(BLUR_BLURTASK, blurTask);

    Job job = new Job(configuration, "Blur Indexer");
    job.setReducerClass(BlurReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BlurMutate.class);
    job.setNumReduceTasks(getNumReducers(configuration));
    return job;
  }

  private void checkTable() {
    ZookeeperClusterStatus status = new ZookeeperClusterStatus(_zooKeeper);
    // check if table exists
    String cluster = _tableDescriptor.cluster;
    String table = _tableDescriptor.name;
    if (!status.exists(false, cluster, table)) {
      throw new RuntimeException("Table [" + table + "] in cluster [" + cluster + "] does not exist.");
    }
    // check if table is locked
    try {
      List<String> children = _zooKeeper.getChildren(ZookeeperPathConstants.getLockPath(cluster, table), false);
      if (!children.isEmpty()) {
        throw new RuntimeException("Table [" + table + "] in cluster [" + cluster
            + "] has write locks enabled, cannot perform update.");
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

  }

  public static BlurTask read(Configuration configuration) throws IOException {
    String base64String = configuration.get(BLUR_BLURTASK);
    if (base64String == null) {
      return null;
    }
    byte[] blurTaskBs = Base64.decodeBase64(base64String);
    BlurTask blurTask = new BlurTask();
    blurTask.readFields(new DataInputStream(new ByteArrayInputStream(blurTaskBs)));
    return blurTask;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    _maxRecordCount = input.readLong();
    _ramBufferSizeMB = input.readInt();
    _optimize = input.readBoolean();
    _indexingType = INDEXING_TYPE.valueOf(readString(input));
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
    output.writeLong(_maxRecordCount);
    output.writeInt(_ramBufferSizeMB);
    output.writeBoolean(_optimize);
    writeString(output, _indexingType.name());
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

  public boolean getOptimize() {
    return _optimize;
  }

  public void setOptimize(boolean optimize) {
    _optimize = optimize;
  }

  public INDEXING_TYPE getIndexingType() {
    return _indexingType;
  }

  public void setIndexingType(INDEXING_TYPE indexingType) {
    _indexingType = indexingType;
  }
}
