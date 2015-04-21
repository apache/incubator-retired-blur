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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class BlurHiveMRLoaderOutputCommitter extends OutputCommitter {

  private static final Log LOG = LogFactory.getLog(BlurHiveMRLoaderOutputCommitter.class);

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {

  }

  @Override
  public void abortJob(JobContext context, int status) throws IOException {
    finishBulkJob(context, false);
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException {

  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    finishBulkJob(context, true);
  }

  private void finishBulkJob(JobContext context, boolean apply) throws IOException {
    Configuration configuration = context.getConfiguration();
    String workingPathStr = configuration.get(BlurSerDe.BLUR_MR_UPDATE_WORKING_PATH);
    Path workingPath = new Path(workingPathStr);
    Path tmpDir = new Path(workingPath, "tmp");
    FileSystem fileSystem = tmpDir.getFileSystem(configuration);
    String loadId = configuration.get(BlurSerDe.BLUR_MR_LOAD_ID);
    Path loadPath = new Path(tmpDir, loadId);

    if (apply) {
      Path newDataPath = new Path(workingPath, "new");
      Path dst = new Path(newDataPath, loadId);
      if (!fileSystem.rename(loadPath, dst)) {
        LOG.error("Could move data from src [" + loadPath + "] to dst [" + dst + "]");
      }
    } else {
      fileSystem.delete(loadPath, true);
    }
  }

}
