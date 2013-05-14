package org.apache.blur.mapreduce.lib;

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
import java.io.IOException;

import org.apache.blur.mapred.AbstractOutputCommitter;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;

public class BlurOutputCommitter extends AbstractOutputCommitter {

  private Path _newIndex;
  private Configuration _configuration;
  private TaskAttemptID _taskAttemptID;
  private Path _indexPath;

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    return true;
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    setup(context);
    FileSystem fileSystem = _newIndex.getFileSystem(_configuration);
    if (fileSystem.exists(_newIndex) && !fileSystem.isFile(_newIndex)) {
      fileSystem.rename(_newIndex, new Path(_indexPath, _taskAttemptID.toString() + ".task_complete"));
    } else {
      throw new IOException("Path [" + _newIndex + "] does not exist, can not commit.");
    }
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    setup(context);
    FileSystem fileSystem = _newIndex.getFileSystem(_configuration);
    fileSystem.delete(_indexPath, true);
  }
  
  private void setup(TaskAttemptContext context) {
    _configuration = context.getConfiguration();
    int shardId = context.getTaskAttemptID().getTaskID().getId();
    _taskAttemptID = context.getTaskAttemptID();
    Path tableOutput = BlurOutputFormat.getOutputPath(_configuration);
    String shardName = BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, shardId);
    _indexPath = new Path(tableOutput, shardName);
    _newIndex = new Path(_indexPath, _taskAttemptID.toString() + ".tmp");
  }

}
