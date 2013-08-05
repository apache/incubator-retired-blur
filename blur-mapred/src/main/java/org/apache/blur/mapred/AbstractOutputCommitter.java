package org.apache.blur.mapred;

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

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptID;

public abstract class AbstractOutputCommitter extends OutputCommitter {

  private final static Log LOG = LogFactory.getLog(AbstractOutputCommitter.class);

  @Override
  public void setupJob(JobContext jobContext) throws IOException {

  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    // look through all the shards for attempts that need to be cleaned up.
    // also find all the attempts that are finished
    // then rename all the attempts jobs to commits
    LOG.info("Commiting Job [{0}]", jobContext.getJobID());
    Configuration configuration = jobContext.getConfiguration();
    Path tableOutput = BlurOutputFormat.getOutputPath(configuration);
    makeSureNoEmptyShards(configuration, tableOutput);
    FileSystem fileSystem = tableOutput.getFileSystem(configuration);
    for (FileStatus fileStatus : fileSystem.listStatus(tableOutput)) {
      if (isShard(fileStatus)) {
        commitOrAbortJob(jobContext, fileStatus.getPath(), true);
      }
    }

  }

  private void makeSureNoEmptyShards(Configuration configuration, Path tableOutput) throws IOException {
    FileSystem fileSystem = tableOutput.getFileSystem(configuration);
    TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(configuration);
    int shardCount = tableDescriptor.getShardCount();
    for (int i = 0; i < shardCount; i++) {
      String shardName = BlurUtil.getShardName(i);
      fileSystem.mkdirs(new Path(tableOutput, shardName));
    }
  }

  private void commitOrAbortJob(JobContext jobContext, Path shardPath, boolean commit) throws IOException {
    FileSystem fileSystem = shardPath.getFileSystem(jobContext.getConfiguration());
    FileStatus[] listStatus = fileSystem.listStatus(shardPath);
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      String name = path.getName();
      boolean taskComplete = name.endsWith(".task_complete");
      if (fileStatus.isDir()) {
        String taskAttemptName = getTaskAttemptName(name);
        TaskAttemptID taskAttemptID = TaskAttemptID.forName(taskAttemptName);
        if (taskAttemptID.getJobID().equals(jobContext.getJobID())) {
          if (commit) {
            if (taskComplete) {
              fileSystem.rename(path, new Path(shardPath, taskAttemptName + ".commit"));
              LOG.info("Committing [{0}] in path [{1}]", taskAttemptID, path);
            } else {
              fileSystem.delete(path, true);
              LOG.info("Deleteing tmp dir [{0}] in path [{1}]", taskAttemptID, path);
            }
          } else {
            fileSystem.delete(path, true);
            LOG.info("Deleteing aborted job dir [{0}] in path [{1}]", taskAttemptID, path);
          }
        }
      }
    }
  }

  private String getTaskAttemptName(String name) {
    int lastIndexOf = name.lastIndexOf('.');
    return name.substring(0, lastIndexOf);
  }

  private boolean isShard(FileStatus fileStatus) {
    return isShard(fileStatus.getPath());
  }

  private boolean isShard(Path path) {
    return path.getName().startsWith(BlurConstants.SHARD_PREFIX);
  }

  @Override
  public void abortJob(JobContext jobContext, int status) throws IOException {
    LOG.info("Abort Job [{0}]", jobContext.getJobID());
    Configuration configuration = jobContext.getConfiguration();
    Path tableOutput = BlurOutputFormat.getOutputPath(configuration);
    makeSureNoEmptyShards(configuration, tableOutput);
    FileSystem fileSystem = tableOutput.getFileSystem(configuration);
    for (FileStatus fileStatus : fileSystem.listStatus(tableOutput)) {
      if (isShard(fileStatus)) {
        commitOrAbortJob(jobContext, fileStatus.getPath(), false);
      }
    }
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException {

  }

}
