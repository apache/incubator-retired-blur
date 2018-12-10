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

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.ShardUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class BlurOutputCommitter extends OutputCommitter {

  private static final Log LOG = LogFactory.getLog(BlurOutputCommitter.class);

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    LOG.info("Running setup job.");
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    // look through all the shards for attempts that need to be cleaned up.
    // also find all the attempts that are finished
    // then rename all the attempts jobs to commits
    LOG.info("Commiting Job [{0}]", jobContext.getJobID());
    Configuration configuration = jobContext.getConfiguration();
    Path tableOutput = BlurOutputFormat.getOutputPath(configuration);
    LOG.info("TableOutput path [{0}]", tableOutput);
    makeSureNoEmptyShards(configuration, tableOutput);
    FileSystem fileSystem = tableOutput.getFileSystem(configuration);
    for (FileStatus fileStatus : fileSystem.listStatus(tableOutput)) {
      LOG.info("Checking file status [{0}] with path [{1}]", fileStatus, fileStatus.getPath());
      if (isShard(fileStatus)) {
        commitOrAbortJob(jobContext, fileStatus.getPath(), true);
      }
    }
    LOG.info("Commiting Complete [{0}]", jobContext.getJobID());
  }

  private void makeSureNoEmptyShards(Configuration configuration, Path tableOutput) throws IOException {
    FileSystem fileSystem = tableOutput.getFileSystem(configuration);
    TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(configuration);
    int shardCount = tableDescriptor.getShardCount();
    for (int i = 0; i < shardCount; i++) {
      String shardName = ShardUtil.getShardName(i);
      fileSystem.mkdirs(new Path(tableOutput, shardName));
    }
  }

  private void commitOrAbortJob(JobContext jobContext, Path shardPath, boolean commit) throws IOException {
    LOG.info("CommitOrAbort [{0}] path [{1}]", commit, shardPath);
    FileSystem fileSystem = shardPath.getFileSystem(jobContext.getConfiguration());
    FileStatus[] listStatus = fileSystem.listStatus(shardPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        LOG.info("Checking path [{0}]", path);
        if (path.getName().endsWith(".task_complete")) {
          return true;
        }
        return false;
      }
    });
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      LOG.info("Trying to commitOrAbort [{0}]", path);
      String name = path.getName();
      boolean taskComplete = name.endsWith(".task_complete");
      if (fileStatus.isDir()) {
        String taskAttemptName = getTaskAttemptName(name);
        if (taskAttemptName == null) {
          LOG.info("Dir name [{0}] not task attempt", name);
          continue;
        }
        TaskAttemptID taskAttemptID = TaskAttemptID.forName(taskAttemptName);
        if (taskAttemptID.getJobID().equals(jobContext.getJobID())) {
          if (commit) {
            if (taskComplete) {
              fileSystem.rename(path, new Path(shardPath, taskAttemptName + ".commit"));
              LOG.info("Committing [{0}] in path [{1}]", taskAttemptID, path);
            } else {
              fileSystem.delete(path, true);
              LOG.info("Deleting tmp dir [{0}] in path [{1}]", taskAttemptID, path);
            }
          } else {
            fileSystem.delete(path, true);
            LOG.info("Deleting aborted job dir [{0}] in path [{1}]", taskAttemptID, path);
          }
        } else {
          LOG.warn("TaskAttempt JobID [{0}] does not match JobContext JobId [{1}]", taskAttemptID.getJobID(),
              jobContext.getJobID());
        }
      }
    }
  }

  private String getTaskAttemptName(String name) {
    int lastIndexOf = name.lastIndexOf('.');
    if (lastIndexOf < 0) {
      return null;
    }
    return name.substring(0, lastIndexOf);
  }

  private boolean isShard(FileStatus fileStatus) {
    return isShard(fileStatus.getPath());
  }

  private boolean isShard(Path path) {
    return path.getName().startsWith(BlurConstants.SHARD_PREFIX);
  }

  @Override
  public void abortJob(JobContext jobContext, State state) throws IOException {
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
    LOG.info("Running job cleanup.");
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    int numReduceTasks = context.getNumReduceTasks();
    TaskAttemptID taskAttemptID = context.getTaskAttemptID();
    return taskAttemptID.isMap() && numReduceTasks != 0 ? false : true;
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    LOG.info("Running task setup.");
  }

  private static class Conf {
    Path _newIndex;
    Configuration _configuration;
    TaskAttemptID _taskAttemptID;
    Path _indexPath;
    TableDescriptor _tableDescriptor;
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    LOG.info("Running commit task.");
    Conf conf = setup(context);
    FileSystem fileSystem = conf._newIndex.getFileSystem(conf._configuration);
    if (fileSystem.exists(conf._newIndex) && !fileSystem.isFile(conf._newIndex)) {
      Path dst = new Path(conf._indexPath, conf._taskAttemptID.toString() + ".task_complete");
      LOG.info("Committing [{0}] to [{1}]", conf._newIndex, dst);
      fileSystem.rename(conf._newIndex, dst);
    } else {
      throw new IOException("Path [" + conf._newIndex + "] does not exist, can not commit.");
    }
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    LOG.info("Running abort task.");
    Conf conf = setup(context);
    FileSystem fileSystem = conf._newIndex.getFileSystem(conf._configuration);
    LOG.info("abortTask - Deleting [{0}]", conf._newIndex);
    fileSystem.delete(conf._newIndex, true);
  }

  private Conf setup(TaskAttemptContext context) throws IOException {
    LOG.info("Setting up committer with task attempt [{0}]", context.getTaskAttemptID().toString());
    Conf conf = new Conf();
    conf._configuration = context.getConfiguration();
    conf._tableDescriptor = BlurOutputFormat.getTableDescriptor(conf._configuration);
    int shardCount = conf._tableDescriptor.getShardCount();
    int attemptId = context.getTaskAttemptID().getTaskID().getId();
    int shardId = attemptId % shardCount;
    conf._taskAttemptID = context.getTaskAttemptID();
    Path tableOutput = BlurOutputFormat.getOutputPath(conf._configuration);
    String shardName = ShardUtil.getShardName(BlurConstants.SHARD_PREFIX, shardId);
    conf._indexPath = new Path(tableOutput, shardName);
    conf._newIndex = new Path(conf._indexPath, conf._taskAttemptID.toString() + ".tmp");
    return conf;
  }

}
