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
package org.apache.blur.hive;

import java.io.IOException;

import org.apache.blur.mapreduce.lib.BlurOutputCommitter;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class BlurHiveOutputCommitter extends OutputCommitter {

  private BlurOutputCommitter _committer = new BlurOutputCommitter();

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    _committer.setupJob(jobContext);
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    _committer.setupTask(taskContext);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    return _committer.needsTaskCommit(taskContext);
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    _committer.commitTask(taskContext);
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    _committer.abortTask(taskContext);
  }

  @Override
  public void abortJob(JobContext jobContext, int status) throws IOException {
    _committer.abortJob(jobContext, null);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void cleanupJob(JobContext context) throws IOException {
    _committer.cleanupJob(context);
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    _committer.commitJob(context);
  }

}
